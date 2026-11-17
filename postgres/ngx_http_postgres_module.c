#include <ngx_core.h>
#include <ngx_http.h>
#include <ngx_config.h>
#include <sys/mman.h>
#include "libpq-fe.h"

#define freeparam {for (int i=0;i<ctx->params[index].nParams;i++) free(paramValues[i]);free(paramValues);}
#define freelogparam {free(logparam[0]);free(logparam[1]);free(logparam[2]);free(logparam[3]);if (logparam[5]!=NULL) free(logparam[5]);}

struct s_param {
    char *storedprocname;
    char *command;
    int nParams;
    char **paramnames;
};


typedef struct pg_ctx_s pg_ctx_t;   /* 前向声明：入口上下文（函数表+连接池） */

typedef struct {
 ngx_str_t conninfo;
 unsigned int storednum;
 struct s_param *params;
 char* conn;
 ngx_int_t poolsize;          /* postgrespoolsize，-1=未配置(用默认16) */
 ngx_str_t reloadtoken;       /* postgresreloadtoken，热重载管理口令 */
 pg_ctx_t *ctx;               /* 入口上下文：函数表+连接池（按入口隔离） */
} ngx_http_postgres_loc_conf_t;

/* ==================== 连接池与热重载（按入口隔离） ====================
 * 每个 postgresconn location 对应一个入口上下文 pg_ctx_t：独立的函数清单、
 * 独立连接池与独立使用统计；相同连接串的多个 location 共享同一入口。
 * worker 内请求串行处理（本模块为同步执行模型），池访问无需加锁。
 *
 * 动态池：target 为“空闲连接保留目标”（可经 /_poolctl 热调）。高峰期池空
 * 即新建连接（借出数不受 target 限制），低谷归还时超出 target 的连接直接
 * 关闭——池随负载自然伸缩；target=0 表示关闭复用（每请求建连，即池开关 OFF）。
 *
 * 热重载：共享内存“代数(generation)”计数器 +1 后，各 worker 在下一个请求
 * 进入时自动重载所有入口的函数清单，全程无需停止/重启 nginx。
 */
#define PGPOOL_MAX 512
#define PGCTX_MAX 16

struct pg_ctx_s {
    char           *conninfo;               /* PQ 连接串 */
    int             idx;                    /* 入口序号（=共享内存 target 数组下标） */
    struct s_param *params;                 /* 函数清单（二分查找表） */
    unsigned int    storednum;              /* 函数个数 */
    PGconn         *pool[PGPOOL_MAX];       /* 空闲连接栈 */
    int             pool_len;
    volatile int    target;                 /* 空闲连接保留目标（热可调，0=关闭复用） */
    int             init_target;            /* 初始 target（on 恢复用） */
    volatile unsigned int gen;              /* 本入口已重载到的代数 */
    /* 使用统计（原子更新，供 /_poolctl 查询与动态调整依据） */
    volatile unsigned int st_get;           /* 取连接总次数 */
    volatile unsigned int st_hit;           /* 命中池 */
    volatile unsigned int st_new;           /* 新建连接 */
    volatile unsigned int st_fail;          /* 建连失败 */
    volatile unsigned int st_drop;          /* 归还时超 target 被关闭 */
    volatile int          st_inuse;         /* 当前借出数 */
};

/* 入口注册表：配置阶段写入（master 进程），fork 后各 worker 只读 */
static pg_ctx_t *g_ctxs[PGCTX_MAX];
static int g_ctxn = 0;

/* 跨 worker 共享内存（master fork 前 mmap，master 存活期内有效）：
 * [0..3]   重载代数 gen
 * [64..]   各入口的池 target（int 数组）——管理接口热改后全 worker 立即生效 */
static volatile unsigned int *g_reload_gen = NULL;
static volatile int *g_sh_target = NULL;    /* 各入口池 target（共享） */
static unsigned int g_local_gen = 0;        /* 本 worker 已重载到的代数 */


/* 配置阶段：按连接串查找/创建入口上下文（相同连接串的 location 共享） */
static pg_ctx_t *pg_ctx_get_or_create(const char *conninfo) {
    for (int i = 0; i < g_ctxn; i++) {
        if (strcmp(g_ctxs[i]->conninfo, conninfo) == 0) return g_ctxs[i];
    }
    if (g_ctxn >= PGCTX_MAX) return NULL;
    pg_ctx_t *c = calloc(1, sizeof(pg_ctx_t));
    if (c == NULL) return NULL;
    c->conninfo = strdup(conninfo);
    c->idx = g_ctxn;
    c->target = 16;
    c->init_target = 16;
    g_ctxs[g_ctxn++] = c;
    return c;
}

static int pg_ctx_index(const pg_ctx_t *ctx) {
    for (int i = 0; i < g_ctxn; i++) {
        if (g_ctxs[i] == ctx) return i;
    }
    return -1;
}

/* 从入口池取连接：优先复用空闲连接，池空则新建（借出数不受 target 限制） */
static PGconn *pgpool_get(pg_ctx_t *ctx, ngx_log_t *log) {
    __atomic_add_fetch(&ctx->st_get, 1, __ATOMIC_RELAXED);
    while (ctx->pool_len > 0) {
        PGconn *c = ctx->pool[--ctx->pool_len];
        if (PQstatus(c) == CONNECTION_OK) {
            __atomic_add_fetch(&ctx->st_hit, 1, __ATOMIC_RELAXED);
            __atomic_add_fetch(&ctx->st_inuse, 1, __ATOMIC_RELAXED);
            return c;
        }
        PQfinish(c);                    /* 池中连接已被服务端断开，丢弃 */
    }
    PGconn *c = PQconnectdb(ctx->conninfo);
    if (c == NULL || PQstatus(c) != CONNECTION_OK) {
        __atomic_add_fetch(&ctx->st_fail, 1, __ATOMIC_RELAXED);
        ngx_log_error(NGX_LOG_ERR, log, 0, "postgres pool: connect failed: %s",
                      c ? PQerrorMessage(c) : "null");
        if (c) PQfinish(c);
        return NULL;
    }
    __atomic_add_fetch(&ctx->st_new, 1, __ATOMIC_RELAXED);
    __atomic_add_fetch(&ctx->st_inuse, 1, __ATOMIC_RELAXED);
    return c;
}

/* 归还连接：清理事务/会话状态后按 target 决定入池或关闭（动态伸缩）。
 * failed=1 表示业务执行出错，事务可能处于 aborted，需先 ROLLBACK。 */
static void pgpool_put(pg_ctx_t *ctx, PGconn *conn, int failed) {
    PGresult *r;
    if (conn == NULL) return;
    __atomic_sub_fetch(&ctx->st_inuse, 1, __ATOMIC_RELAXED);
    if (failed) {
        r = PQexec(conn, "ROLLBACK");
        if (r) PQclear(r);
    }
    /* 清除 check_login 设置的会话级审计变量，避免串号到下一请求 */
    r = PQexec(conn, "SELECT set_config('app.operatorid','',false)");
    if (r) PQclear(r);
    if (PQstatus(conn) != CONNECTION_OK) { PQfinish(conn); return; }
    /* target 存于共享内存：任一 worker 经 /_poolctl 热调后全 worker 立即生效 */
    int t = g_sh_target ? g_sh_target[ctx->idx] : ctx->target;
    if (t > 0 && ctx->pool_len < t && ctx->pool_len < PGPOOL_MAX) {
        ctx->pool[ctx->pool_len++] = conn;
    } else {
        __atomic_add_fetch(&ctx->st_drop, 1, __ATOMIC_RELAXED);
        PQfinish(conn);                 /* 超出目标/池已关：关闭（低谷自然收缩） */
    }
}


/* 从查询串/body 中提取 name=value（不做 URL 解码，token 建议用 hex 字符） */
static int pg_get_arg(u_char *data, unsigned int len, const char *name,
                      u_char *out, unsigned int outlen) {
    unsigned int nlen = strlen(name), i = 0;
    while (i < len) {
        u_char *amp = memchr(data + i, '&', len - i);
        unsigned int end = amp ? (unsigned int)(amp - data) : len;
        if (end > i + nlen + 1 && memcmp(data + i, name, nlen) == 0
            && data[i + nlen] == '=') {
            unsigned int vlen = end - i - nlen - 1;
            if (vlen >= outlen) return 0;
            memcpy(out, data + i + nlen + 1, vlen);
            out[vlen] = 0;
            return 1;
        }
        i = end + 1;
    }
    return 0;
}

static void pg_free_params(struct s_param *params, unsigned int count) {
    if (params == NULL) return;
    for (unsigned int i = 0; i < count; i++) {
        free(params[i].storedprocname);
        free(params[i].command);
        if (params[i].paramnames) {
            for (int j = 0; j < params[i].nParams; j++) free(params[i].paramnames[j]);
            free(params[i].paramnames);
        }
    }
    free(params);
}


/* 加载函数清单（配置阶段与运行期热重载共用）。
 * 成功返回 0 并填充 out_num 与 out_params；失败返回 -1。 */
static int pg_load_functions(char *conninfo, ngx_log_t *log,
                             unsigned int *out_num, struct s_param **out_params) {
    PGconn *conn = PQconnectdb(conninfo);
    struct s_param *params = NULL;
    unsigned int n = 0;
    unsigned int i = 0;
    PGresult *res;
    *out_params = NULL; *out_num = 0;
    if (conn == NULL || PQstatus(conn) != CONNECTION_OK) {
        ngx_log_error(NGX_LOG_EMERG, log, 0, "postgres module: connect failed: %s",
                      conn ? PQerrorMessage(conn) : "null");
        if (conn) PQfinish(conn);
        return -1;
    }
    res = PQexec(conn, "select count(*) from pg_proc t1 left join pg_namespace t2 on t1.pronamespace=t2.oid where t2.nspname not in ('pg_catalog','information_schema','private','cron','myself','crypto','gm')");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        PQclear(res); PQfinish(conn); return -1;
    }
    n = atoi(PQgetvalue(res, 0, 0));
    PQclear(res);
    res = PQexec(conn, "select string_agg('$'||sort1||'::'||typname,',' order by sort1) params,func,proargnames,pronargs from (select row_number() over(partition by func order by sort) sort1,t3.typname typname,t.func,t.pronargs,proargnames from (select row_number() over() sort,t.* from (select ' '||t2.nspname||'.'||t1.proname func,t1.pronargs,substr(proargnames::varchar,2,char_length(proargnames::varchar)-2)||',' proargnames,proargtypes,regexp_split_to_table(t1.proargtypes::varchar,' ') aa from pg_proc t1 left join pg_namespace t2 on t1.pronamespace=t2.oid  where t2.nspname not in ('pg_catalog','information_schema','private','cron','myself','crypto','gm')) t ) t left join pg_type t3 on t.aa=t3.oid::varchar ) t group by func,pronargs,proargnames order by func");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        PQclear(res); PQfinish(conn); return -1;
    }
    ngx_log_error(NGX_LOG_NOTICE, log, 0, "postgres module: translate %d functions", PQntuples(res));
    {
        unsigned int nf = (unsigned int)PQntuples(res);
        if (nf != n) {
            ngx_log_error(NGX_LOG_WARN, log, 0,
                "postgres module: find %d functions but translate %d, using %d",
                (int)n, (int)nf, (int)nf);
        }
        n = nf;
    }
    if (n > 0) params = (struct s_param *)malloc(sizeof(struct s_param) * n);
    for (i = 0; i < n; i++) {
        char *temp1 = PQgetvalue(res, i, 1);
        char *k = PQgetvalue(res, i, 2);
        char *temp2 = PQgetvalue(res, i, 0);
        int len1, len2;
        params[i].storedprocname = NULL; params[i].command = NULL; params[i].paramnames = NULL;
        if (temp1 == NULL || k == NULL || temp2 == NULL) {
            ngx_log_error(NGX_LOG_EMERG, log, 0, "postgres module: null column at row %u", i);
            goto fail_row;
        }
        len1 = strlen(temp1); len2 = strlen(temp2);
        params[i].storedprocname = (char*)malloc(len1 + 1);
        params[i].storedprocname[len1] = 0;
        memcpy(params[i].storedprocname, temp1, len1);
        params[i].storedprocname[0] = 47;              /* 前导空格 -> '/' */
        params[i].nParams = atoi(PQgetvalue(res, i, 3));
        if (params[i].nParams > 0 && k[0] == 0) {
            ngx_log_error(NGX_LOG_EMERG, log, 0,
                "postgres module: %s declares %d args but has no argnames",
                params[i].storedprocname, params[i].nParams);
            goto fail_row;
        }
        params[i].command = (char*)malloc(9 + len1 + len2);
        params[i].command[8 + len1 + len2] = 0;
        memcpy(params[i].command, "select", 6);
        memcpy(params[i].command + 6, temp1, len1);
        params[i].command[6 + len1] = '(';
        memcpy(params[i].command + 7 + len1, temp2, len2);
        params[i].command[7 + len1 + len2] = ')';
        *strchr(params[i].storedprocname, '.') = 47;   /* 'schema.func' -> 'schema/func' */
        params[i].paramnames = (char**)malloc((params[i].nParams > 0 ? params[i].nParams : 1) * sizeof(char*));
        for (int j = 0; j < params[i].nParams; j++) {
            char *k1 = strchr(k, ',');
            if (k1 == NULL || (k1 - k) < 3) {
                ngx_log_error(NGX_LOG_EMERG, log, 0,
                    "postgres module: malformed argname token for %s (arg %d)",
                    params[i].storedprocname, j);
                params[i].nParams = j;                 /* 仅保留已构造的 argname */
                goto fail_row;
            }
            params[i].paramnames[j] = (char*)malloc(k1 - k - 1);
            memcpy(params[i].paramnames[j], k + 2, k1 - k - 2);
            params[i].paramnames[j][k1 - k - 2] = 0;
            k = k1 + 1;
        }
    }
    PQclear(res);
    PQfinish(conn);
    *out_num = n;
    *out_params = params;
    return 0;
fail_row:
    PQclear(res);
    PQfinish(conn);
    pg_free_params(params, i + 1);
    return -1;
}


/* 热重载单个入口的函数清单。失败时保留旧表并推进该入口代数，
 * 避免数据库故障期间每个请求都重试造成雪崩。 */
static void pg_ctx_reload(pg_ctx_t *ctx, ngx_log_t *log) {
    unsigned int n = 0;
    struct s_param *p = NULL;
    unsigned int want = g_reload_gen ? *g_reload_gen : (ctx->gen + 1);
    if (pg_load_functions(ctx->conninfo, log, &n, &p) != 0) {
        ngx_log_error(NGX_LOG_ERR, log, 0,
                      "postgres module: hot reload failed for ctx %d, keeping old function table",
                      pg_ctx_index(ctx));
        ctx->gen = want;
        return;
    }
    pg_free_params(ctx->params, ctx->storednum);
    ctx->params = p;
    ctx->storednum = n;
    ctx->gen = want;
    ngx_log_error(NGX_LOG_NOTICE, log, 0,
                  "postgres module: hot reload ok for ctx %d (%s), %d functions",
                  pg_ctx_index(ctx), ctx->conninfo, (int)n);
}

/* 热重载所有入口 */
static void pg_reload_all(ngx_log_t *log) {
    for (int i = 0; i < g_ctxn; i++) pg_ctx_reload(g_ctxs[i], log);
    if (g_reload_gen) g_local_gen = *g_reload_gen;
}

/* master：fork 前分配跨 worker 共享页（gen + 各入口池 target），配置值写入共享页 */
static ngx_int_t postgres_init_module(ngx_cycle_t *cycle) {
    if (g_reload_gen == NULL) {
        void *m = mmap(NULL, 4096, PROT_READ | PROT_WRITE,
                       MAP_SHARED | MAP_ANONYMOUS, -1, 0);
        if (m == MAP_FAILED) {
            ngx_log_error(NGX_LOG_EMERG, cycle->log, ngx_errno,
                          "postgres module: mmap reload generation failed");
            return NGX_ERROR;
        }
        memset(m, 0, 4096);
        g_reload_gen = (volatile unsigned int *)m;
        g_sh_target = (volatile int *)((char *)m + 64);
    }
    /* 每次（重）加载配置：以配置值为基线刷新各入口的共享 target */
    for (int i = 0; i < g_ctxn; i++) g_sh_target[i] = g_ctxs[i]->target;
    *g_reload_gen = 0;
    return NGX_OK;
}

/* worker 启动：同步代数，避免启动即触发一次无谓重载 */
static ngx_int_t postgres_init_process(ngx_cycle_t *cycle) {
    g_local_gen = g_reload_gen ? *g_reload_gen : 0;
    for (int i = 0; i < g_ctxn; i++) g_ctxs[i]->gen = g_local_gen;
    ngx_log_error(NGX_LOG_NOTICE, cycle->log, 0,
                  "postgres module: worker started, ctxs=%d gen=%u",
                  g_ctxn, g_local_gen);
    return NGX_OK;
}

/* worker 退出：关闭各入口池中全部空闲连接并释放函数表 */
static void postgres_exit_process(ngx_cycle_t *cycle) {
    for (int i = 0; i < g_ctxn; i++) {
        pg_ctx_t *c = g_ctxs[i];
        while (c->pool_len > 0) PQfinish(c->pool[--c->pool_len]);
        pg_free_params(c->params, c->storednum);
        c->params = NULL; c->storednum = 0;
    }
}



ngx_module_t ngx_http_postgres_module ;

void setparam(unsigned int len,u_char *c,int index,char** paramValues,u_char* logparam[6],struct s_param *params) {
    unsigned int i=0;
    while (i<=len) {
        u_char *j=memchr(c+i,'&',len-i);
        if (j==NULL) j=c+len-1;
        else j--;
        u_char *k=memchr(c+i,'=',j-c-i+1);
        char * arg=malloc(k-c-i+1);
        memcpy(arg,c+i,k-c-i);
        arg[k-c-i]=0;
        if (k!=NULL&&j-k>=1) {
            for (int i1=0;i1<params[index].nParams;i1++) {
                if (strcmp(params[index].paramnames[i1],arg)==0) {
                    paramValues[i1]=malloc(j-k+1);
                    int i2=0,i3=0;
                    while (i2<j-k) {
                        u_char *ch=k+1+i2,ch1=0,ch2=0;
                        switch(*ch) {
                        case '+':
                            paramValues[i1][i3]=' ';
                            i2++;
                            i3++;
                            break;
                        case '%':
                            if (*(ch+1)>='0'&&*(ch+1)<='9') ch1=*(ch+1)-'0';
                            else if (*(ch+1)>='a'&&*(ch+1)<='z') ch1=*(ch+1)-'a'+10;
                            else if (*(ch+1)>='A'&&*(ch+1)<='Z') ch1=*(ch+1)-'A'+10;
                            else { i2+=2;break; }
                            if (*(ch+2)>='0'&&*(ch+2)<='9') ch2=*(ch+2)-'0';
                            else if (*(ch+2)>='a'&&*(ch+2)<='z') ch2=*(ch+2)-'a'+10;
                            else if (*(ch+2)>='A'&&*(ch+2)<='Z') ch2=*(ch+2)-'A'+10;
                            else { i2+=3;break; }
                            paramValues[i1][i3]=(char)((ch1<<4)|ch2);
                            i3++;
                            i2+=3;
                            break;
                        default:
                            paramValues[i1][i3]=*ch;
                            i2++;
                            i3++;
                            break;
                        }
                    }
                    paramValues[i1][i3]=0;
                    if (strcmp("operatorno",arg)==0) {
                        logparam[5]=malloc(i3+1);
                        memcpy(logparam[5],paramValues[i1],i3);
                    }
                }

            }
        }
        i=j-c+2;
        free(arg);
    }

}

static ngx_int_t ngx_http_procpostgres_process(ngx_http_request_t *req);
static void ngx_http_procpostgres_body_handler(ngx_http_request_t *req);

/* 修复：POST/PUT 必须先等请求体完整到达再处理。
 * 原实现直接从 req->connection->buffer 读取 body，当请求头与请求体分属
 * 不同的读事件（浏览器、Python urllib 等常见）时，处理器会在 body 尚未
 * 到达时被调用，读到空 body，导致 token 等参数丢失，表现为间歇性的
 * “非法登录(100005) / 子系统信息出错(100012)”。 */
static ngx_int_t ngx_http_procpostgres_handler(ngx_http_request_t *req) {
    ngx_int_t rc;
    if (req->method & (NGX_HTTP_POST|NGX_HTTP_PUT)) {
        rc = ngx_http_read_client_request_body(req,
                                               ngx_http_procpostgres_body_handler);
        if (rc >= NGX_HTTP_SPECIAL_RESPONSE) { return rc; }
        return NGX_DONE;
    }
    return ngx_http_procpostgres_process(req);
}

static void ngx_http_procpostgres_body_handler(ngx_http_request_t *req) {
    ngx_int_t rc;
    /* 关键修复：在 ngx_http_read_client_request_body 的异步回调里，
       必须显式调用 ngx_http_finalize_request 结束请求。
       此前丢弃返回值导致请求永不终结：
         · keepalive 连接被污染，复用后后续请求挂起（“POST 无响应”）；
         · 后续请求 body 解析错乱，参数(含 token)丢失；
         · access.log 不记录该请求。 */
    rc = ngx_http_procpostgres_process(req);
    ngx_http_finalize_request(req, rc);
}

/* 以指定状态码返回一段 JSON 文本（text 需存活至 output_filter 完成） */
static ngx_int_t pg_send_text(ngx_http_request_t *req, int status, const char *text) {
    size_t n = strlen(text);
    req->headers_out.status = status;
    ngx_str_set(&req->headers_out.content_type, "application/json;charset=utf-8");
    ngx_http_send_header(req);
    ngx_buf_t *b = ngx_pcalloc(req->pool, sizeof(ngx_buf_t));
    b->pos = (u_char*)text;
    b->last = (u_char*)text + n;
    b->memory = 1;
    b->last_buf = 1;
    ngx_chain_t out;
    out.buf = b;
    out.next = NULL;
    return ngx_http_output_filter(req, &out);
}

/* 鉴权：配置了 postgresreloadtoken 则必须匹配；未配置仅允许本机(127.0.0.1) */
static int pg_check_admin(ngx_http_request_t *req, ngx_http_postgres_loc_conf_t *lc) {
    u_char tok[128];
    if (lc->reloadtoken.len > 0) {
        if (req->args.len == 0 || req->args.len >= 512) return 0;
        if (!pg_get_arg(req->args.data, req->args.len, "token", tok, sizeof(tok))) return 0;
        return ngx_strlen(tok) == lc->reloadtoken.len
            && ngx_strncmp(tok, lc->reloadtoken.data, lc->reloadtoken.len) == 0;
    }
    return (req->connection->addr_text.len == 9
            && ngx_strncmp(req->connection->addr_text.data, "127.0.0.1", 9) == 0);
}

/* conninfo 脱敏：password=*** */
static void pg_mask_conninfo(const char *ci, char *out, size_t outlen) {
    char *p = strstr((char *)ci, "password=");
    if (p == NULL) { snprintf(out, outlen, "%s", ci); return; }
    char *q = p + 9;
    while (*q && *q != ' ') q++;
    snprintf(out, outlen, "%.*s***%s", (int)(p + 9 - ci), ci, q);
}

/* /_poolctl：入口连接池热管理（按入口隔离的函数表/连接池）
 *   GET  /_poolctl                                  查看所有入口池状态与统计
 *   POST /_poolctl?action=resize&conn=0&target=32   热调某入口空闲连接目标
 *   POST /_poolctl?action=on&conn=0                 恢复该入口初始池大小
 *   POST /_poolctl?action=off&conn=0                关闭复用（排空，target=0）
 *   POST /_poolctl?action=reload[&conn=0]           重载函数清单（默认全部入口）
 * conn 可为入口序号或连接串子串；鉴权同 /_reloadfunctions。 */
static ngx_int_t pg_poolctl(ngx_http_request_t *req, ngx_http_postgres_loc_conf_t *lc) {
    (void)lc;
    u_char act[32] = "status", conn[64] = "", tgt[32] = "";
    if (req->args.len > 0 && req->args.len < 512) {
        pg_get_arg(req->args.data, req->args.len, "action", act, sizeof(act));
        pg_get_arg(req->args.data, req->args.len, "conn", conn, sizeof(conn));
        pg_get_arg(req->args.data, req->args.len, "target", tgt, sizeof(tgt));
    }
    int idx = -1;
    if (conn[0]) {
        if (conn[0] >= '0' && conn[0] <= '9') {
            idx = atoi((char *)conn);
            if (idx < 0 || idx >= g_ctxn) idx = -1;
        } else {
            for (int i = 0; i < g_ctxn; i++) {
                if (strstr(g_ctxs[i]->conninfo, (char *)conn)) { idx = i; break; }
            }
        }
        if (idx < 0) return pg_send_text(req, 404, "{\"errorcode\":1,\"message\":\"ctx not found\"}");
    }
    char msg[256];
    if (strcmp((char *)act, "resize") == 0) {
        if (idx < 0 || tgt[0] == 0) return pg_send_text(req, 400, "{\"errorcode\":1,\"message\":\"need conn & target\"}");
        int n = atoi((char *)tgt);
        if (n < 0 || n > PGPOOL_MAX) return pg_send_text(req, 400, "{\"errorcode\":1,\"message\":\"target out of range\"}");
        g_ctxs[idx]->target = n;
        if (g_sh_target) g_sh_target[idx] = n;   /* 写共享页：全 worker 立即生效 */
        snprintf(msg, sizeof(msg), "{\"errorcode\":0,\"message\":\"resized\",\"ctx\":%d,\"target\":%d}", idx, n);
        return pg_send_text(req, 200, msg);
    }
    if (strcmp((char *)act, "on") == 0 || strcmp((char *)act, "off") == 0) {
        if (idx < 0) return pg_send_text(req, 400, "{\"errorcode\":1,\"message\":\"need conn\"}");
        int on = (act[1] == 'n');
        g_ctxs[idx]->target = on ? g_ctxs[idx]->init_target : 0;
        if (g_sh_target) g_sh_target[idx] = g_ctxs[idx]->target;   /* 写共享页：全 worker 生效 */
        snprintf(msg, sizeof(msg), "{\"errorcode\":0,\"message\":\"%s\",\"ctx\":%d,\"target\":%d}",
                 on ? "pool on" : "pool off (draining)", idx, g_ctxs[idx]->target);
        return pg_send_text(req, 200, msg);
    }
    if (strcmp((char *)act, "reload") == 0) {
        if (g_reload_gen) __atomic_add_fetch((unsigned int *)g_reload_gen, 1, __ATOMIC_SEQ_CST);
        if (idx >= 0) {
            pg_ctx_reload(g_ctxs[idx], req->connection->log);
            snprintf(msg, sizeof(msg), "{\"errorcode\":0,\"message\":\"reloaded\",\"ctx\":%d,\"functions\":%u}",
                     idx, g_ctxs[idx]->storednum);
        } else {
            pg_reload_all(req->connection->log);
            unsigned int total = 0;
            for (int i = 0; i < g_ctxn; i++) total += g_ctxs[i]->storednum;
            snprintf(msg, sizeof(msg), "{\"errorcode\":0,\"message\":\"reloaded\",\"ctxs\":%d,\"functions\":%u}",
                     g_ctxn, total);
        }
        return pg_send_text(req, 200, msg);
    }
    /* 默认 action=status：输出所有入口池状态与使用统计 */
    {
        char buf[8192];
        int m = snprintf(buf, sizeof(buf), "{\"errorcode\":0,\"ctxs\":[");
        for (int i = 0; i < g_ctxn && m < (int)sizeof(buf) - 420; i++) {
            pg_ctx_t *c = g_ctxs[i];
            char masked[256];
            pg_mask_conninfo(c->conninfo, masked, sizeof(masked));
            m += snprintf(buf + m, sizeof(buf) - m,
                "%s{\"ctx\":%d,\"conn\":\"%s\",\"functions\":%u,\"target\":%d,"
                "\"idle\":%d,\"inuse\":%d,\"get\":%u,\"hit\":%u,\"new\":%u,"
                "\"fail\":%u,\"drop\":%u,\"gen\":%u}",
                i ? "," : "", i, masked, c->storednum,
                g_sh_target ? g_sh_target[c->idx] : c->target, c->pool_len,
                c->st_inuse < 0 ? 0 : c->st_inuse, c->st_get, c->st_hit,
                c->st_new, c->st_fail, c->st_drop, c->gen);
        }
        snprintf(buf + m, sizeof(buf) - m, "]}");
        return pg_send_text(req, 200, buf);
    }
}

static ngx_int_t ngx_http_procpostgres_process(ngx_http_request_t *req) {
    PGconn *conn;
    unsigned int len=req->uri.len;
    short setp=1;
    ngx_http_postgres_loc_conf_t* local_conf=ngx_http_get_module_loc_conf(req,ngx_http_postgres_module);
    pg_ctx_t *ctx = local_conf->ctx;          /* 本 location 的入口（函数表+连接池） */
    /* ---- 连接池热管理接口：/_poolctl ---- */
    if (req->uri.len == 9 && ngx_strncmp(req->uri.data, "/_poolctl", 9) == 0) {
        if (!pg_check_admin(req, local_conf)) {
            return pg_send_text(req, 403,
                "{\"errorcode\":100005,\"message\":\"admin token invalid\"}");
        }
        return pg_poolctl(req, local_conf);
    }
    /* ---- 热重载管理接口：GET/POST /_reloadfunctions?token=xxxx ---- */
    if (req->uri.len == 17 && ngx_strncmp(req->uri.data, "/_reloadfunctions", 17) == 0) {
        if (!pg_check_admin(req, local_conf)) {
            return pg_send_text(req, 403,
                "{\"errorcode\":100005,\"message\":\"reload token invalid\"}");
        }
        if (g_reload_gen) {
            __atomic_add_fetch((unsigned int*)g_reload_gen, 1, __ATOMIC_SEQ_CST);
            pg_reload_all(req->connection->log);   /* 本 worker 立即生效 */
            unsigned int total = 0;
            for (int i = 0; i < g_ctxn; i++) total += g_ctxs[i]->storednum;
            u_char *msg = ngx_pnalloc(req->pool, 96);
            snprintf((char*)msg, 96,
                     "{\"errorcode\":0,\"message\":\"functions reloaded\",\"functions\":%u}",
                     total);
            return pg_send_text(req, 200, (const char*)msg);
        }
        return pg_send_text(req, 500, "{\"errorcode\":1,\"message\":\"reload not ready\"}");
    }
    /* ---- 各 worker 按入口在下一个请求进入时自动跟上最新代数 ---- */
    if (ctx && g_reload_gen && ctx->gen != *g_reload_gen) {
        pg_ctx_reload(ctx, req->connection->log);
    }
    if (ctx == NULL) {                         /* 该 location 未配置 postgresconn */
        return NGX_ERROR;
    }
    u_char* logparam[6];
    logparam[1]=malloc(len+1);//1--uri
    logparam[1][len]=0;
    logparam[5]=NULL;//5--opeartor
    memcpy(logparam[1],req->uri.data,len);
    logparam[0]=malloc(req->connection->addr_text.len+1);//0 clinetip
    logparam[0][req->connection->addr_text.len]=0;
    memcpy(logparam[0],req->connection->addr_text.data,req->connection->addr_text.len);

    ngx_log_error(NGX_LOG_DEBUG, req->connection->log, 0, (const char*)logparam[1]);
    for (unsigned int i=0;i<len;i++) {
        if (req->uri.data[i]>='A'&&req->uri.data[i]<='Z') logparam[1][i]+=32;
    }
    int index=0,low=0,high=(int)ctx->storednum-1,cmp=0;
    while (low<=high) {
        index=(low+high)/2;
        cmp=strcmp((const char*)logparam[1],ctx->params[index].storedprocname);
        if (cmp==0) break;
        if (cmp>0) low=index+1;
        else high=index-1;
    }
    if (cmp!=0) {   /* 接口不存在：返回标准 JSON 错误（9997，落在 100~10000 系统错误段） */
        free(logparam[1]);free(logparam[0]);
        return pg_send_text(req,404,"{\"errorcode\":9997,\"message\":\"接口不存在！\",\"info\":null}");
    }
    char **paramValues;
//    const char* paramValues[ctx->params[index].nParams];
    unsigned int i=0;
    paramValues=(char**)calloc(ctx->params[index].nParams,sizeof(char*));
    logparam[3]=NULL;//params
    if (req->args.len>0) {
        logparam[3]=malloc(req->args.len+1);
        logparam[3][req->args.len]=0;
        memcpy(logparam[3],req->args.data,req->args.len);
      ngx_log_error(NGX_LOG_DEBUG,req->connection->log,0,(const char*)logparam[3]);
      for (int i1=0;i1<ctx->params[index].nParams;i1++) {
        if (strcmp(ctx->params[index].paramnames[i1],"httpparams")==0) {
          paramValues[i1]=malloc(len+1);
          paramValues[i1][len]=0;
          memcpy(paramValues[i1],logparam[3],len);
          setp=0;
          break;
        }
      }
      if (setp)
      setparam(req->args.len,logparam[3],index,paramValues,logparam,ctx->params);
    }
    /* 修复：body 从已完整读取的 request_body 获取（见上方 wrapper）。
     * 直接读 req->connection->buffer 会因请求体尚未到达/被拆分而丢参。 */
    len=0;
    logparam[2]=NULL;//body
    setp=1;
    if (req->request_body && req->request_body->bufs) {
        ngx_chain_t *cl;
        for (cl=req->request_body->bufs; cl; cl=cl->next) {
            if (cl->buf) len += (unsigned int)(cl->buf->last - cl->buf->pos);
        }
    }
    if (len>0) {
        ngx_chain_t *cl;
        unsigned int off=0;
        logparam[2]=malloc(len+1); //2--body
        logparam[2][len]=0;
        for (cl=req->request_body->bufs; cl; cl=cl->next) {
            if (cl->buf) {
                unsigned int s=(unsigned int)(cl->buf->last - cl->buf->pos);
                memcpy(logparam[2]+off, cl->buf->pos, s);
                off+=s;
            }
        }
      ngx_log_error(NGX_LOG_DEBUG,req->connection->log,0,(const char*)logparam[2]);
      for (int i1=0;i1<ctx->params[index].nParams;i1++) {
        if (strcmp(ctx->params[index].paramnames[i1],"httpbody")==0) {
          paramValues[i1]=malloc(len+1);
          paramValues[i1][len]=0;
          memcpy(paramValues[i1],logparam[2],len);
          setp=0;
          break;
        }
      }
      if (setp)
      setparam(len,logparam[2],index,paramValues,logparam,ctx->params);
    }
    /* 从连接池取连接（池空或禁用时才真正建连） */
    conn=pgpool_get(ctx, req->connection->log);
    if (conn==NULL) {freeparam;freelogparam;return NGX_ERROR;}
//    const char *const * p=paramValues;
    PGresult *res=PQexecParams(conn,ctx->params[index].command,ctx->params[index].nParams,NULL, (const char *const *)paramValues,NULL,NULL,0);
    ExecStatusType et=PQresultStatus(res);
    freeparam;
//    ngx_log_error(NGX_LOG_DEBUG,req->connection->log,0,"exec");
    unsigned char* aa;
    if (et!=PGRES_TUPLES_OK) {
        /* 执行出错：返回标准 JSON 错误结构（errorcode 落在 100~10000 系统错误段）。
         * 9998 = 服务器执行出错（SQL 异常、参数类型不匹配等），登记于 sysinfo.errorcode。
         * 错误消息需做 JSON 字符串转义，避免破坏响应结构。 */
        const char *pmsg=PQresultErrorField(res,PG_DIAG_MESSAGE_PRIMARY);
        const char *fmsg=PQresultErrorMessage(res);
        const char *emsg=(pmsg&&pmsg[0])?pmsg:((fmsg&&fmsg[0])?fmsg:"execute error");
        size_t ml=strlen(emsg);
        size_t cap=ml*6+64;
        size_t w=0;
        aa=(unsigned char*)ngx_pnalloc(req->pool,cap);
        const char *hdr="{\"errorcode\":9998,\"message\":\"";
        size_t hl=strlen(hdr);
        memcpy(aa,hdr,hl); w=hl;
        for (size_t k=0;k<ml && w+8<cap;k++) {
            unsigned char ch=((const unsigned char*)emsg)[k];
            if (ch=='"'||ch=='\\') { aa[w++]='\\'; aa[w++]=ch; }
            else if (ch=='\n') { aa[w++]='\\'; aa[w++]='n'; }
            else if (ch=='\r') { aa[w++]='\\'; aa[w++]='r'; }
            else if (ch=='\t') { aa[w++]='\\'; aa[w++]='t'; }
            else if (ch<0x20) { w+=(size_t)snprintf((char*)aa+w,cap-w,"\\u%04x",(unsigned)ch); }
            else aa[w++]=ch;
        }
        memcpy(aa+w,"\",\"info\":null}",14); w+=14;
        aa[w]=0;
        i=(unsigned int)w;
        ngx_log_error(NGX_LOG_ERR, req->connection->log, 0, "postgres exec error: %s", emsg);
    } else {
      const char *val=PQgetvalue(res,0,0);
      i=(val?strlen(val):0);
      aa=(unsigned char*)ngx_pnalloc(req->pool,i+1);
      aa[i]=0;
      if (val) memcpy(aa,val,i);
    }
    if (et!=PGRES_TUPLES_OK) {          /* 业务失败：先清理 aborted 事务，随后的日志写入才能成功 */
        PGresult *rb=PQexec(conn,"ROLLBACK");
        if (rb) PQclear(rb);
    }
    logparam[4]=aa;
    {
        PGresult *logres=PQexecParams(conn,"insert into public.serverlog(clientip,funcname,content,logtime,serverip,params,res,operatorid) values($1,$2,$3,now(),inet_client_addr(),$4,$5,$6);",6,NULL,(const char *const*)logparam,NULL,NULL,0);
        if (logres) PQclear(logres);
    }
    freelogparam;
    req->headers_out.status = 200;
    ngx_str_set(&req->headers_out.content_type, "application/json;charset=utf-8");
    ngx_http_send_header(req);
    ngx_buf_t *b;
    b = ngx_pcalloc(req->pool, sizeof(ngx_buf_t));
    b->pos = aa;
    b->last = aa + i;
    b->memory = 1;
    b->last_buf = 1;
    ngx_chain_t out;
    out.buf = b;
    out.next = NULL;
    PQclear(res);
    pgpool_put(ctx, conn, 0);   /* 归还连接池（内部已重置会话状态） */
    return ngx_http_output_filter(req, &out);
};

static void *ngx_http_postgres_create_loc_conf(ngx_conf_t *cf) {
    ngx_http_postgres_loc_conf_t *local_conf =NULL;
    local_conf = ngx_pcalloc( cf->pool, sizeof(ngx_http_postgres_loc_conf_t) );
    if ( local_conf == NULL )  { return NULL;}
    // 初始设置默认值
    ngx_str_null(&local_conf->conninfo);
    local_conf->storednum=0;
    local_conf->conn=NULL;
    local_conf->params=NULL;
    local_conf->poolsize=-1;
    ngx_str_null(&local_conf->reloadtoken);
    local_conf->ctx=NULL;
    return local_conf;
}

static char *ngx_http_postgres_set(ngx_conf_t *cf,ngx_command_t *cmd, void *conf) {
    ngx_http_postgres_loc_conf_t* local_conf=conf;
    char *rv=ngx_conf_set_str_slot(cf,cmd,conf);
    if (local_conf->conninfo.len>0) {
        local_conf->conn=malloc(local_conf->conninfo.len+1);
        local_conf->conn[local_conf->conninfo.len]=0;
        memcpy(local_conf->conn,local_conf->conninfo.data,local_conf->conninfo.len);
        ngx_conf_log_error(NGX_LOG_NOTICE,cf,0,"postgresql connection:%s",local_conf->conn);
        /* 函数清单/连接池按入口隔离：相同连接串的 location 共享同一入口上下文 */
        pg_ctx_t *ctx = pg_ctx_get_or_create(local_conf->conn);
        if (ctx == NULL) {
            ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "postgres module: too many postgresconn entries (max %d)", PGCTX_MAX);
            return NGX_CONF_ERROR;
        }
        local_conf->ctx = ctx;
        if (local_conf->poolsize != -1) {
            ctx->target = (int)local_conf->poolsize;
            ctx->init_target = ctx->target;
        }
        if (ctx->params == NULL) {          /* 该入口首次出现：加载函数清单 */
            if (pg_load_functions(local_conf->conn, cf->log, &ctx->storednum, &ctx->params) != 0) {
                return NGX_CONF_ERROR;
            }
        }
        ngx_conf_log_error(NGX_LOG_NOTICE, cf, 0, "progresql init ok: ctx %d, %d functions, pool size %d",
                           pg_ctx_index(ctx), (int)ctx->storednum, ctx->target);
          ngx_http_core_loc_conf_t *corecf;
          corecf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
          corecf->handler = ngx_http_procpostgres_handler;
    }
    return rv;
}


static ngx_command_t postgres_commands[] = {
    {
        ngx_string("postgresconn"),
        NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
        ngx_http_postgres_set,
        NGX_HTTP_LOC_CONF_OFFSET,
        0,
        NULL
    },
    {
        ngx_string("postgrespoolsize"),
        NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
        ngx_conf_set_num_slot,
        NGX_HTTP_LOC_CONF_OFFSET,
        offsetof(ngx_http_postgres_loc_conf_t, poolsize),
        NULL
    },
    {
        ngx_string("postgresreloadtoken"),
        NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
        ngx_conf_set_str_slot,
        NGX_HTTP_LOC_CONF_OFFSET,
        offsetof(ngx_http_postgres_loc_conf_t, reloadtoken),
        NULL
    },
    ngx_null_command
};

static ngx_http_module_t ngx_http_postgres_module_ctx = {
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    ngx_http_postgres_create_loc_conf,
    NULL
};


ngx_module_t ngx_http_postgres_module = {
  NGX_MODULE_V1,
  &ngx_http_postgres_module_ctx,
  postgres_commands,
  NGX_HTTP_MODULE,
  NULL,                    /* init master */
  postgres_init_module,    /* init module：分配跨 worker 共享的重载代数页 */
  postgres_init_process,   /* init process：worker 同步代数 */
  NULL,                    /* init thread */
  NULL,                    /* exit thread */
  postgres_exit_process,   /* exit process：清空连接池 */
  NULL,                    /* exit master */
  NGX_MODULE_V1_PADDING
};

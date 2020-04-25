#include <ngx_core.h>
#include <ngx_http.h>
#include <ngx_config.h>
#include <libpq-fe.h>
#include <pthread.h>
#include <sys/time.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <stdlib.h>

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;/*³õÊ¼»¯»¥³âËø*/
pthread_cond_t  cond = PTHREAD_COND_INITIALIZER;//init cond
struct s_conn {
	PGconn *conn;
    struct s_conn *n;
};
struct s_param {
    char *storedprocname;
    char *command;
    int nParams;
    char **paramnames;
};
struct s_param *params;
static unsigned int storednum=0;
//static bool lockconn=false;
//static bool unlockconn=false;
//static struct connchain *useconn=NULL;
//static struct connchain *unuseconn=NULL;

static char* conninfo;
long timeout_ms = 1000; // wait time 1000ms
static struct s_conn *punconn=NULL;

static PGconn* getconn() {
    struct timespec abstime;
    struct timeval now;
    gettimeofday(&now, NULL);
    long nsec = now.tv_usec * 1000 + (timeout_ms % 1000) * 1000000;
    abstime.tv_sec=now.tv_sec + nsec / 1000000000 + timeout_ms / 1000;
    abstime.tv_nsec=nsec % 1000000000;
    int ret=pthread_cond_timedwait(&cond, &mutex, &abstime);
    if (ret!=0) {
        PGconn *conn=PQconnectdb(conninfo);
        if ((ConnStatusType)PQstatus(conn)!=CONNECTION_OK) {PQfinish(conn);return NULL;}
        return conn;
    }
    pthread_mutex_lock(&mutex);
    struct s_conn *tempconn=punconn;
    while (tempconn!=NULL) {
        PGconn *conn=tempconn->conn;
        if (PQstatus(conn)==CONNECTION_OK) {
            punconn=tempconn->n;
            pthread_mutex_unlock(&mutex);
            free(tempconn);
            return conn;
        }
        PQfinish(conn);
        tempconn=tempconn->n;
        free(punconn);
        punconn=tempconn;
    }
    pthread_mutex_unlock(&mutex);
    PGconn *conn=PQconnectdb(conninfo);
    if ((ConnStatusType)PQstatus(conn)!=CONNECTION_OK) {PQfinish(conn);return NULL;}
    return conn;
}

static void freeconn(PGconn *conn) {
    struct timespec abstime;
    struct timeval now;
    gettimeofday(&now, NULL);
    long nsec = now.tv_usec * 1000 + (timeout_ms % 1000) * 1000000;
    abstime.tv_sec=now.tv_sec + nsec / 1000000000 + timeout_ms / 1000;
    abstime.tv_nsec=nsec % 1000000000;
    int ret=pthread_cond_timedwait(&cond, &mutex, &abstime);
    if (ret!=0) {
        PQfinish(conn);
        return;
    }
    pthread_mutex_lock(&mutex);
    struct s_conn *newconn=malloc(sizeof(struct s_conn));
    newconn->conn=conn;
    newconn->n=punconn;
    punconn=newconn;
    pthread_mutex_unlock(&mutex);
}

static ngx_int_t init_func(ngx_cycle_t *cycle) {
    char buf[2048];
    char *p;
    static PGconn *conn;
    FILE *fn=NULL;
    fn=fopen("postgres.conf","r");
    if (fn==NULL) {fclose(fn) ;return NGX_ERROR;}
    while (!feof(fn)) {
        memset(buf,0,sizeof(buf));
        if (fgets(buf,2048,fn)==NULL) continue;
        if (strncmp(buf,"conn=",5)!=0) continue;
        if (conninfo!=NULL) free(conninfo);
        if (buf[strlen(buf)-1]=='\n') buf[strlen(buf)-1]=0;
        p=strchr(buf,0);
        if (p==NULL) continue;
        conninfo=(char *)malloc(p-buf-5);
        strncpy(conninfo,buf+5,p-buf-5);
    }
    fclose(fn);
    if (conninfo==NULL) return NGX_ERROR;
    conn=getconn();
    if (conn==NULL) {return NGX_ERROR;}
//    if ((ConnStatusType)PQstatus(conn)!=CONNECTION_OK) {PQfinish(conn);return NGX_ERROR;}
    if (params!=NULL) {
        for (unsigned int i=0;i<storednum;i++) {
            free(params[i].storedprocname);
            free(params[i].command);
            free(params[i].paramnames);
            for (int j=0;j<params[i].nParams;j++) {
                free(params[i].paramnames);
            }
        }
        free(params);
    }
    PGresult *res=PQexec(conn,"select count(*) from pg_proc t1 left join pg_namespace t2 on t1.pronamespace=t2.oid where t2.nspname not in ('pg_catalog','information_schema')");
    if (PQresultStatus(res)!=PGRES_TUPLES_OK) {PQclear(res);freeconn(conn);return NGX_ERROR;}
    storednum=atoi(PQgetvalue(res,0,0));
    params=(struct s_param *)malloc(sizeof(struct s_param)*storednum);
    PQclear(res);
//    res=PQexec(conn,"select params,' '||nspname||'.'||proname func,substr(proargnames::varchar,2,char_length(proargnames::varchar)-2)||',' proargnames,pronargs from (select string_agg(sort,',') params,oid from (select '$'||row_number() over(partition by oid)||'::'||t3.typname  sort,t.oid from (select t1.oid,regexp_split_to_table(t1.proargtypes::varchar,' ') aa from pg_proc t1 left join pg_namespace t2 on t1.pronamespace=t2.oid where t2.nspname not in ('pg_catalog','information_schema')) t  left join pg_type t3 on t.aa=t3.oid::varchar) t group by oid) t left join pg_proc t1 on t1.oid=t.oid left join pg_namespace t2 on t1.pronamespace=t2.oid order by func");
    res=PQexec(conn,"select string_agg('$'||sort1||'::'||typname,',' order by sort1) params,func,proargnames,pronargs from (select row_number() over(partition by func order by sort) sort1,t3.typname typname,t.func,t.pronargs,proargnames from (select row_number() over() sort,t.* from (select ' '||t2.nspname||'.'||t1.proname func,t1.pronargs,substr(proargnames::varchar,2,char_length(proargnames::varchar)-2)||',' proargnames,proargtypes,regexp_split_to_table(t1.proargtypes::varchar,' ') aa from pg_proc t1 left join pg_namespace t2 on t1.pronamespace=t2.oid  where t2.nspname not in ('pg_catalog','information_schema')) t ) t left join pg_type t3 on t.aa=t3.oid::varchar ) t group by func,pronargs,proargnames");
    if (PQresultStatus(res)!=PGRES_TUPLES_OK) {PQclear(res);freeconn(conn);return NGX_ERROR;}
    for (unsigned int i=0;i<storednum;i++) {
        char *temp1 = PQgetvalue(res,i,1);
        int len1 = strlen(temp1);
//        printf("temp1=%s, len1=%d\n",temp1,len1);
        params[i].storedprocname=(char*)malloc(len1+1);
        strcpy(params[i].storedprocname,temp1);
        //memcpy(params[i].storedprocname,PQgetvalue(res,i,1),strlen(PQgetvalue(res,i,1)));
        //params[i].storedprocname[strlen(PQgetvalue(res,i,1))]=0;
        params[i].storedprocname[0]=47;
        params[i].nParams=atoi(PQgetvalue(res,i,3));
        char *temp2 = PQgetvalue(res,i,0);
        int len2 = strlen(temp2);
//        printf("temp2=%s, len2=%d\n",temp2,len2);
        params[i].command=(char*)malloc(9+len1+len2);
//         strcpy(params[i].command,"select");
        strcpy(params[i].command,"select");
        //memcpy(params[i].command,"select",6);
        strcpy(params[i].command+6,temp1);
        strcpy(params[i].command+6+len1,"(");
        //memcpy(params[i].command+6,PQgetvalue(res,i,1),strlen(PQgetvalue(res,i,1)));
        //params[i].command[6+strlen(PQgetvalue(res,i,1))]='(';
        strcpy(params[i].command+7+len1,temp2);
        //memcpy(params[i].command+7+strlen(PQgetvalue(res,i,1)),PQgetvalue(res,i,0),strlen(PQgetvalue(res,i,0)));
        strcpy(params[i].command+7+len1+len2,")");
        //params[i].command[7+strlen(PQgetvalue(res,i,1))+strlen(PQgetvalue(res,i,0))]=')';
        //params[i].command[8+strlen(PQgetvalue(res,i,1))+strlen(PQgetvalue(res,i,0))]=0;
//        params[i].command="select operator.login($1::varchar,$2::varchar,$3::varchar,$4::numeric)";
        params[i].paramnames=(char**)malloc(params[i].nParams*sizeof(char*));
        *strchr(params[i].storedprocname,'.')=47;
        char *k=PQgetvalue(res,i,2);
        for (int j=0;j<params[i].nParams;j++) {
            char *k1=strchr(k,',');
            params[i].paramnames[j]=(char*)malloc(k1-k-1);
            memcpy(params[i].paramnames[j],k+2,k1-k-2);
            params[i].paramnames[j][k1-k-2]=0;
            k=k1+1;
        }
    }
    PQclear(res);
    freeconn(conn);
    return NGX_OK;
}

static ngx_int_t ngx_http_procpostgres_handler(ngx_http_request_t *req) {
  //u_char html[1024]=req->uri.data;
  //strng t=ngx_string(strcpy(strcpy(strcpy(strcpy("<h1>This is Test Page</h1><div>",req->uri.data),"</div><div>"),req->args.data),"</div"));
  //std::string t="<h1>This is Test Page</h1><div>"+req->uri.data+"</div><div>"+req->args.data+"</div";
  //int len = sizeof(html) - 1;
    static PGconn *conn;
    unsigned int len=req->uri.len;
    char* uri=malloc(len+1);
    memcpy(uri,req->uri.data,len);
    uri[len]=0;
    for (int i=0;i<len;i++) {
        if (req->uri.data[i]>='A'&&req->uri.data[i]<='Z') uri[i]+=32;
    }
    int index,low=0,high=storednum-1,cmp;
    while (low<=high) {
        index=(low+high)/2;
//        if (len<strlen(params[index].storedprocname)) len=strlen(params[index].storedprocname);
        cmp=strcmp(uri,params[index].storedprocname);
        if (cmp==0) break;
        if (cmp>0) low=index+1;
        else high=index-1;
    }
    free(uri);
    if (cmp!=0) return NGX_ERROR;
    char **paramValues;
    paramValues=(char**)calloc(params[index].nParams,sizeof(char*));
    unsigned int i=0;
    while (i<=req->args.len) {
        u_char *j=memchr(req->args.data+i,'&',req->args.len-i);
        if (j==NULL) j=req->args.data+req->args.len-1;
        else j--;
        u_char *k=memchr(req->args.data+i,'=',j-req->args.data-i+1);
        char * arg=malloc(k-req->args.data-i+1);
        memcpy(arg,req->args.data+i,k-req->args.data-i);
        arg[k-req->args.data-i]=0;
        if (k!=NULL&&j-k>1) {
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
                }
            }
        }
        i=j-req->args.data+2;
        free(arg);
    }
    conn=getconn();
    if (conn==NULL) {ngx_log_error(NGX_LOG_EMERG, req->connection->log, 0, "out of connnect!");return NGX_ERROR;}
    PGresult *res=PQexecParams(conn,params[index].command,params[index].nParams,NULL,paramValues,NULL,NULL,0);
    freeconn(conn);
    ExecStatusType et=PQresultStatus(res);
    for (int i=0;i<params[index].nParams;i++) free(paramValues[i]);free(paramValues);
    if (et!=PGRES_TUPLES_OK) {
        ngx_log_error(NGX_LOG_EMERG, req->connection->log, 0, PQresultErrorField(res,PG_DIAG_MESSAGE_PRIMARY),PQresultErrorField(res,PG_DIAG_INTERNAL_QUERY));
        PQclear(res);
        return NGX_ERROR;
    }
    u_char* aa;
    aa=malloc(strlen(PQgetvalue(res,0,0))+1);
    memcpy(aa,PQgetvalue(res,0,0),strlen(PQgetvalue(res,0,0)));
    req->headers_out.status = 200;
//    req->headers_out.content_length_n = strlen(PQgetvalue(res,0,0));
    ngx_str_set(&req->headers_out.content_type, "text/html;charset=utf-8");
    ngx_http_send_header(req);
    ngx_buf_t *b;
    b = ngx_pcalloc(req->pool, sizeof(ngx_buf_t));
    b->pos = aa;
    b->last = aa + strlen(PQgetvalue(res,0,0));
  //  b->pos = t.data;
  //  b->last = t.data+t.len;
    b->memory = 1;
    b->last_buf = 1;
    ngx_chain_t out;
    out.buf = b;
    out.next = NULL;
    PQclear(res);
    return ngx_http_output_filter(req, &out);
};

static ngx_int_t ngx_http_querypostgres_handler(ngx_http_request_t *req) {
    static PGconn *conn;
    unsigned int len=req->uri.len;
    char* uri=malloc(len);
    memcpy(uri,req->uri.data+1,len-1);
    uri[len-1]=0;
    conn=getconn();
    if (conn==NULL) {ngx_log_error(NGX_LOG_EMERG, req->connection->log, 0, "out of connnect!");return NGX_ERROR;}
    PGresult *res=PQexec(conn,uri);
    freeconn(conn);
    ExecStatusType et=PQresultStatus(res);
    char* aa;
    if (et==PGRES_EMPTY_QUERY ) {
        aa=malloc(strlen("empty query!"));
        strcpy(aa,"empty query!");
    }
    else if (et==PGRES_COMMAND_OK ) {
        aa=malloc(strlen("command ok"));
        strcpy(aa,"command ok");

    }
    else if (et!=PGRES_TUPLES_OK&&et!=PGRES_SINGLE_TUPLE) {
        int len1=strlen(PQresultErrorField(res,PG_DIAG_MESSAGE_PRIMARY));
        int len2=0;
        if (PQresultErrorField(res,PG_DIAG_INTERNAL_QUERY)!=NULL) len+=strlen(PQresultErrorField(res,PG_DIAG_INTERNAL_QUERY));
        aa=malloc(len1+len2+1);
        strcpy(aa,PQresultErrorField(res,PG_DIAG_MESSAGE_PRIMARY));
        aa[len1+len2]=10;
        if (len2!=0)
            strcpy(aa+strlen(PQresultErrorField(res,PG_DIAG_MESSAGE_PRIMARY))+1,PQresultErrorField(res,PG_DIAG_INTERNAL_QUERY));
    } else {
        int row=PQntuples(res),col=PQnfields(res),n=15;
        for (int i=0;i<col;i++) n+=strlen(PQfname(res,i));
        for (int i=0;i<row;i++) {
            for (int j=0;j<col;j++)
                n+=strlen(PQgetvalue(res,i,j));
        }
        n+=(col+1)*(row+1)*9;
        aa=malloc(n);
        strcpy(aa,"<table><tr>");
        int m=10;
        for (int i=0;i<col;i++) {
            strcpy(aa+m,"<td>");
            strcpy(aa+m+4,PQfname(res,i));
            strcpy(aa+m+4+strlen(PQfname(res,i)),"</td>");
            m+=4+strlen(PQfname(res,i))+5;
        }
        strcpy(aa+m,"</tr>");
        m+=5;
        for (int i=0;i<row;i++) {
            strcpy(aa+m,"<tr>");
            m+=4;
            for (int j=0;j<col;j++) {
                strcpy(aa+m,"<td>");
                strcpy(aa+m+4,PQgetvalue(res,i,j));
                strcpy(aa+m+4+strlen(PQgetvalue(res,i,j)),"</td>");
                m+=4+strlen(PQgetvalue(res,i,j))+5;
            }
            strcpy(aa+m,"</tr>");
            m+=5;
        }
        strcpy(aa+m,"</table>");
    }
    req->headers_out.status = 200;
//    req->headers_out.content_length_n = strlen(aa);
    ngx_str_set(&req->headers_out.content_type, "text/html;charset=utf-8");
    ngx_http_send_header(req);
    ngx_buf_t *b;
    b = ngx_pcalloc(req->pool, sizeof(ngx_buf_t));
    b->pos = aa;
    b->last = aa + strlen(aa);
  //  b->pos = t.data;
  //  b->last = t.data+t.len;
    b->memory = 1;
    b->last_buf = 1;
    ngx_chain_t out;
    out.buf = b;
    out.next = NULL;
    PQclear(res);
    return ngx_http_output_filter(req, &out);

}

static char *ngx_http_procpostgres(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  ngx_http_core_loc_conf_t *corecf;
  corecf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
  corecf->handler = ngx_http_procpostgres_handler;
  return NGX_CONF_OK;
}

static char *ngx_http_querypostgres(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  ngx_http_core_loc_conf_t *corecf;
  corecf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
  corecf->handler = ngx_http_querypostgres_handler;
  return NGX_CONF_OK;
}

static ngx_command_t postgres_commands[] = {
    {
        ngx_string("procpostgres"),
        NGX_HTTP_LOC_CONF | NGX_CONF_ANY,
        ngx_http_procpostgres,
        NGX_HTTP_LOC_CONF_OFFSET,
        0,
        NULL
    },
    {
        ngx_string("querypostgres"),
        NGX_HTTP_LOC_CONF | NGX_CONF_ANY,
        ngx_http_querypostgres,
        NGX_HTTP_LOC_CONF_OFFSET,
        0,
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
    NULL,
    NULL
};

ngx_module_t ngx_http_postgres_module = {
  NGX_MODULE_V1,
  &ngx_http_postgres_module_ctx,
  postgres_commands,
  NGX_HTTP_MODULE,
  NULL, init_func ,NULL,  NULL, NULL, NULL, NULL,
  NGX_MODULE_V1_PADDING
};



#include <ngx_core.h>
#include <ngx_http.h>
#include <ngx_config.h>
#include "libpq-fe.h"

#define freeparam {for (int i=0;i<local_conf->params[index].nParams;i++) free(paramValues[i]);free(paramValues);}
#define freelogparam {free(logparam[0]);free(logparam[1]);free(logparam[2]);free(logparam[3]);if (logparam[5]==NULL) free(logparam[5]);}

struct s_param {
    char *storedprocname;
    char *command;
    int nParams;
    char **paramnames;
};


typedef struct {
 ngx_str_t conninfo;
 unsigned int storednum;
 struct s_param *params;
 char* conn;
} ngx_http_postgres_loc_conf_t;

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

static ngx_int_t ngx_http_procpostgres_handler(ngx_http_request_t *req) {
    static PGconn *conn;
    unsigned int len=req->uri.len;
    short setp=1;
    ngx_http_postgres_loc_conf_t* local_conf=ngx_http_get_module_loc_conf(req,ngx_http_postgres_module);
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
    int index=0,low=0,high=local_conf->storednum-1,cmp=0;
    while (low<=high) {
        index=(low+high)/2;
        cmp=strcmp((const char*)logparam[1],local_conf->params[index].storedprocname);
        if (cmp==0) break;
        if (cmp>0) low=index+1;
        else high=index-1;
    }
    if (cmp!=0) {free(logparam[1]);free(logparam[0]);return NGX_ERROR;}
    char **paramValues;
//    const char* paramValues[local_conf->params[index].nParams];
    unsigned int i=0;
    paramValues=(char**)calloc(local_conf->params[index].nParams,sizeof(char*));
    logparam[3]=NULL;//params
    if (req->args.len>0) {
        logparam[3]=malloc(req->args.len+1);
        logparam[3][req->args.len]=0;
        memcpy(logparam[3],req->args.data,req->args.len);
      ngx_log_error(NGX_LOG_DEBUG,req->connection->log,0,(const char*)logparam[3]);
      for (int i1=0;i1<local_conf->params[index].nParams;i1++) {
        if (strcmp(local_conf->params[index].paramnames[i1],"httpparams")==0) {
          paramValues[i1]=malloc(len+1);
          paramValues[i1][len]=0;
          memcpy(paramValues[i1],logparam[3],len);
          setp=0;
          break;
        }
      }
      if (setp)
      setparam(req->args.len,logparam[3],index,paramValues,logparam,local_conf->params);
    }
    len=req->connection->buffer->last-req->connection->buffer->pos;
    logparam[2]=NULL;//body
    setp=1;
    if (len>0) {
        logparam[2]=malloc(len+1); //2--body
        logparam[2][len]=0;
        memcpy(logparam[2],req->connection->buffer->pos,len);
      ngx_log_error(NGX_LOG_DEBUG,req->connection->log,0,(const char*)logparam[2]);
      for (int i1=0;i1<local_conf->params[index].nParams;i1++) {
        if (strcmp(local_conf->params[index].paramnames[i1],"httpbody")==0) {
          paramValues[i1]=malloc(len+1);
          paramValues[i1][len]=0;
          memcpy(paramValues[i1],logparam[2],len);
          setp=0;
          break;
        }
      }
      if (setp)
      setparam(len,logparam[2],index,paramValues,logparam,local_conf->params);
    }
    conn=PQconnectdb(local_conf->conn);
//    ngx_log_error(NGX_LOG_DEBUG,req->connection->log,0,"encoding:%s",pg_encoding_to_char(PQclientEncoding(conn)));
    if ((ConnStatusType)PQstatus(conn)!=CONNECTION_OK) {PQfinish(conn);freeparam;return NGX_ERROR;}
//    const char *const * p=paramValues;
    PGresult *res=PQexecParams(conn,local_conf->params[index].command,local_conf->params[index].nParams,NULL, (const char *const *)paramValues,NULL,NULL,0);
    ExecStatusType et=PQresultStatus(res);
    freeparam;
//    ngx_log_error(NGX_LOG_DEBUG,req->connection->log,0,"exec");
    unsigned char* aa;
    if (et!=PGRES_TUPLES_OK) {
        ngx_log_error(NGX_LOG_EMERG, req->connection->log, 0, PQresultErrorField(res,PG_DIAG_MESSAGE_PRIMARY),PQresultErrorField(res,PG_DIAG_INTERNAL_QUERY));
        i=strlen(PQresultErrorMessage(res));
        aa=(unsigned char*)malloc(i+1);
        aa[i]=0;
        memcpy(aa,PQresultErrorMessage(res),strlen(PQresultErrorMessage(res)));
    } else {
      i=strlen(PQgetvalue(res,0,0));
      aa=malloc(i+1);
      aa[i]=0;
      memcpy(aa,PQgetvalue(res,0,0),strlen(PQgetvalue(res,0,0)));
    }
    logparam[4]=aa;
    PQexecParams(conn,"insert into public.serverlog(clientip,funcname,content,logtime,serverip,params,res,operatorid) values($1,$2,$3,now(),inet_client_addr(),$4,$5,$6);",6,NULL,(const char *const*)logparam,NULL,NULL,0);
    freelogparam;
    req->headers_out.status = 200;
    ngx_str_set(&req->headers_out.content_type, "text/html;charset=utf-8");
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
    PQfinish(conn);
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
        PGconn *conn;
        conn=PQconnectdb(local_conf->conn);
        if ((ConnStatusType)PQstatus(conn)!=CONNECTION_OK) {PQfinish(conn);return NGX_CONF_ERROR ;}
        ngx_conf_log_error(NGX_LOG_NOTICE,cf,0,"connect ok");
//        ngx_conf_log_error(NGX_LOG_NOTICE,cf,0,"encoding:%s",pg_encoding_to_char(PQclientEncoding(conn)));
        PGresult *res=PQexec(conn,"select count(*) from pg_proc t1 left join pg_namespace t2 on t1.pronamespace=t2.oid where t2.nspname not in ('pg_catalog','information_schema','private','cron','myself')");
        if (PQresultStatus(res)!=PGRES_TUPLES_OK) {
            PQclear(res);
            PQfinish(conn);
            return NGX_CONF_ERROR;
        }
        local_conf->storednum=atoi(PQgetvalue(res,0,0));
        local_conf->params=(struct s_param *)malloc(sizeof(struct s_param)*local_conf->storednum);
        PQclear(res);
        ngx_conf_log_error(NGX_LOG_NOTICE,cf,0,"find %d functions",local_conf->storednum);
        res=PQexec(conn,"select string_agg('$'||sort1||'::'||typname,',' order by sort1) params,func,proargnames,pronargs from (select row_number() over(partition by func order by sort) sort1,t3.typname typname,t.func,t.pronargs,proargnames from (select row_number() over() sort,t.* from (select ' '||t2.nspname||'.'||t1.proname func,t1.pronargs,substr(proargnames::varchar,2,char_length(proargnames::varchar)-2)||',' proargnames,proargtypes,regexp_split_to_table(t1.proargtypes::varchar,' ') aa from pg_proc t1 left join pg_namespace t2 on t1.pronamespace=t2.oid  where t2.nspname not in ('pg_catalog','information_schema','private','cron','myself')) t ) t left join pg_type t3 on t.aa=t3.oid::varchar ) t group by func,pronargs,proargnames");
        if (PQresultStatus(res)!=PGRES_TUPLES_OK) {
            PQclear(res);
            PQfinish(conn);
            return NGX_CONF_ERROR;
        }
        ngx_conf_log_error(NGX_LOG_NOTICE,cf,0,"translate %d functions",PQntuples(res));
        for (unsigned int i=0;i<local_conf->storednum;i++) {
            char *temp1 = PQgetvalue(res,i,1);
            int len1 = strlen(temp1);
            local_conf->params[i].storedprocname=(char*)malloc(len1+1);
            local_conf->params[i].storedprocname[len1]=0;
            memcpy(local_conf->params[i].storedprocname,temp1,len1);
            local_conf->params[i].storedprocname[0]=47;
            local_conf->params[i].nParams=atoi(PQgetvalue(res,i,3));
            char *temp2 = PQgetvalue(res,i,0);
            int len2 = strlen(temp2);
            local_conf->params[i].command=(char*)malloc(9+len1+len2);
            local_conf->params[i].command[8+len1+len2]=0;
            memcpy(local_conf->params[i].command,"select",6);
            memcpy(local_conf->params[i].command+6,temp1,len1);
            local_conf->params[i].command[6+len1]='(';
            memcpy(local_conf->params[i].command+7+len1,temp2,len2);
            local_conf->params[i].command[7+len1+len2]=')';
            local_conf->params[i].paramnames=(char**)malloc(local_conf->params[i].nParams*sizeof(char*));
            *strchr(local_conf->params[i].storedprocname,'.')=47;
            char *k=PQgetvalue(res,i,2);
            for (int j=0;j<local_conf->params[i].nParams;j++) {
                char *k1=strchr(k,',');
                local_conf->params[i].paramnames[j]=(char*)malloc(k1-k-1);
                memcpy(local_conf->params[i].paramnames[j],k+2,k1-k-2);
                local_conf->params[i].paramnames[j][k1-k-2]=0;
                k=k1+1;
            }
//            ngx_log_error(NGX_LOG_DEBUG,cycle->log,0,params[i].storedprocname);
        }
        ngx_conf_log_error(NGX_LOG_NOTICE,cf,0,"progresql init ok");
        PQclear(res);
        PQfinish(conn);
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
  NULL, NULL ,NULL,  NULL, NULL, NULL, NULL,
  NGX_MODULE_V1_PADDING
};


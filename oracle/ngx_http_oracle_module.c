#include <ngx_core.h>
#include <ngx_http.h>
#include <ngx_config.h>
#include <oci.h>
//#include <ocilib.h>
#include <libpq-fe.h>
#include <pthread.h>
#include <sys/time.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <stdlib.h>

pthread_mutex_t oraclemutex = PTHREAD_MUTEX_INITIALIZER;/*³õÊ¼»¯»¥³âËø*/
pthread_cond_t  oraclecond = PTHREAD_COND_INITIALIZER;//init cond
struct s_conn {
	PGconn *conn;
    struct s_conn *n;
};
struct s_param {
    char *storedprocname;
    char *command;
    int nParams;
    char **paramnames;
} *params;
static unsigned int storednum=0;
//static bool lockconn=false;
//static bool unlockconn=false;
//static struct connchain *useconn=NULL;
//static struct connchain *unuseconn=NULL;

static char* conninfo;
static struct s_conninfo {
    char* oracle_db;
    char* oracle_user;
    char* oracle_pass;
    OCISPool *poolhp;
    OCIEnv *envhp;
    OCIAuthInfo *authp;
    int timeout;
    OraText *poolName;
    ub4 poolNameLen;
    OCISvcCtx *svchp;
    struct s_param *params;
} *con;
long oracletimeout_ms = 1000; // wait time 1000ms
unsigned int MAX_CONN=100;
static struct s_conn *punconn=NULL;


static PGconn* getconn() {
    struct timespec abstime;
    struct timeval now;
    gettimeofday(&now, NULL);
    long nsec = now.tv_usec * 1000 + (oracletimeout_ms % 1000) * 1000000;
    abstime.tv_sec=now.tv_sec + nsec / 1000000000 + oracletimeout_ms / 1000;
    abstime.tv_nsec=nsec % 1000000000;
    int ret=pthread_cond_timedwait(&oraclecond, &oraclemutex, &abstime);
    if (ret!=0) {
        PGconn *conn=PQconnectdb(conninfo);
        if ((ConnStatusType)PQstatus(conn)!=CONNECTION_OK) {
            PQfinish(conn);
            return NULL;
        }
        return conn;
    }
    pthread_mutex_lock(&oraclemutex);
    struct s_conn *tempconn=punconn;
    while (tempconn!=NULL) {
        PGconn *conn=tempconn->conn;
        if (PQstatus(conn)==CONNECTION_OK) {
            punconn=tempconn->n;
            pthread_mutex_unlock(&oraclemutex);
            free(tempconn);
            return conn;
        }
        PQfinish(conn);
        tempconn=tempconn->n;
        free(punconn);
        punconn=tempconn;
    }
    pthread_mutex_unlock(&oraclemutex);
    PGconn *conn=PQconnectdb(conninfo);
    if ((ConnStatusType)PQstatus(conn)!=CONNECTION_OK) {PQfinish(conn);return NULL;}
    return conn;
}

static void freeconn(PGconn *conn) {
    struct timespec abstime;
    struct timeval now;
    gettimeofday(&now, NULL);
    long nsec = now.tv_usec * 1000 + (oracletimeout_ms % 1000) * 1000000;
    abstime.tv_sec=now.tv_sec + nsec / 1000000000 + oracletimeout_ms / 1000;
    abstime.tv_nsec=nsec % 1000000000;
    int ret=pthread_cond_timedwait(&oraclecond, &oraclemutex, &abstime);
    if (ret!=0) {
        PQfinish(conn);
        return;
    }
    pthread_mutex_lock(&oraclemutex);
    struct s_conn *newconn=malloc(sizeof(struct s_conn));
    newconn->conn=conn;
    newconn->n=punconn;
    punconn=newconn;
    pthread_mutex_unlock(&oraclemutex);
}

void checkerr(OCIError *errhp, sword status,const char* message) {
    text errbuf[512];
    sb4 errcode = 0;
    if (status!=OCI_SUCCESS)  printf("%s",message);
    switch (status)
    {
    case OCI_SUCCESS:
      break;
    case OCI_SUCCESS_WITH_INFO:
      (void) printf("Error - OCI_SUCCESS_WITH_INFO\n");
      break;
    case OCI_NEED_DATA:
      (void) printf("Error - OCI_NEED_DATA\n");
      break;
    case OCI_NO_DATA:
      (void) printf("Error - OCI_NODATA\n");
      break;
    case OCI_ERROR:
      (void) OCIErrorGet((dvoid *)errhp, (ub4) 1, (text *) NULL, &errcode,
                          errbuf, (ub4) sizeof(errbuf), OCI_HTYPE_ERROR);
      (void) printf("Error - %.*s\n", 512, errbuf);
      break;
    case OCI_INVALID_HANDLE:
      (void) printf("Error - OCI_INVALID_HANDLE\n");
      break;
    case OCI_STILL_EXECUTING:
      (void) printf("Error - OCI_STILL_EXECUTE\n");
      break;
    case OCI_CONTINUE:
      (void) printf("Error - OCI_CONTINUE\n");
      break;
    default:
      break;
    }
}

static void excesql(char * sql) {
    sword r;
    OCIError *errhp;
    OCIHandleAlloc((dvoid *)con->envhp, (dvoid **)&errhp, OCI_HTYPE_ERROR,0,(void **)0);
    OCISvcCtx *svchp = (OCISvcCtx *) 0;
    OCISessionGet(con->envhp, errhp, &svchp, con->authp,
                       (OraText *)con->poolName, (ub4)strlen((char *)con->poolName), NULL,
                       0, NULL, NULL, NULL, OCI_SESSGET_SPOOL);
    OCIStmt *stmthp = (OCIStmt *)0;
    OCIHandleAlloc(con->envhp, (dvoid**)&stmthp,OCI_HTYPE_STMT, 0, (dvoid **)0);
    OCIStmtPrepare2(svchp, &stmthp, errhp, (oratext *)sql, strlen(sql), NULL, 0, OCI_NTV_SYNTAX, OCI_DEFAULT);
    ub4 rtype;
    OCIAttrGet((void *)stmthp, OCI_HTYPE_STMT, &rtype, 0, OCI_ATTR_STMT_TYPE, errhp);
    if (rtype == OCI_RESULT_TYPE_SELECT)
    {
        checkerr(errhp,OCIStmtExecute(svchp, stmthp, errhp, 0, 0, (const OCISnapshot *)0, (OCISnapshot *)0, OCI_DEFAULT),"exec:");
        /* Perform normal OCI actions to define and fetch rows. */
        int pcount;
        OCIAttrGet((void*) stmthp, (ub4) OCI_HTYPE_STMT, (void*) &pcount,(ub4 *) 0, (ub4) OCI_ATTR_PARAM_COUNT, (OCIError *) errhp  );
        ub2          dtype;
        text         *col_name;
        ub4          col_name_len, char_semantics;
        ub2          col_width;
        for (int i=1;i<=pcount;i++) {
            OCIParam     *mypard = (OCIParam *) 0;
            checkerr(errhp, OCIParamGet((void *)stmthp, OCI_HTYPE_STMT, errhp, (void **)&mypard, (ub4) i),"getparam");
            checkerr(errhp, OCIAttrGet((void*) mypard, (ub4) OCI_DTYPE_PARAM,
                    (void*) &dtype,(ub4 *) 0, (ub4) OCI_ATTR_DATA_TYPE,
                    (OCIError *) errhp  ),"data_type:");
            /* Retrieve the column name attribute */
            col_name_len = 0;
            checkerr(errhp, OCIAttrGet((void*) mypard, (ub4) OCI_DTYPE_PARAM,
                    (void**) &col_name, (ub4 *) &col_name_len, (ub4) OCI_ATTR_NAME,
                    (OCIError *) errhp ),"colname");
            /* Retrieve the length semantics for the column */
            char_semantics = 0;
            checkerr(errhp, OCIAttrGet((void*) mypard, (ub4) OCI_DTYPE_PARAM,
                    (void*) &char_semantics,(ub4 *) 0, (ub4) OCI_ATTR_CHAR_USED,
                    (OCIError *) errhp  ),"ischar");
            col_width = 0;
            if (char_semantics)
                /* Retrieve the column width in characters */
                checkerr(errhp, OCIAttrGet((void*) mypard, (ub4) OCI_DTYPE_PARAM,
                        (void*) &col_width, (ub4 *) 0, (ub4) OCI_ATTR_CHAR_SIZE,
                        (OCIError *) errhp  ),"strlen");
            else
                /* Retrieve the column width in bytes */
                checkerr(errhp, OCIAttrGet((void*) mypard, (ub4) OCI_DTYPE_PARAM,
                        (void*) &col_width,(ub4 *) 0, (ub4) OCI_ATTR_DATA_SIZE,
                        (OCIError *) errhp  ),"length");

        }
        //    OCIParam     *mypard = (OCIParam *) 0;
        //    ub2          dtype;
        //    text         *col_name;
        //    ub4          counter, col_name_len, char_semantics;
        //    ub2          col_width;
        //    sb4          parm_status;
        //    counter = 1;
        //    parm_status = OCIParamGet((void *)stmthp, OCI_HTYPE_STMT, con->errhp,
        //                   (void **)&mypard, (ub4) counter);
            /* Loop only if a descriptor was successfully retrieved for
               current position, starting at 1 */
        //    while (parm_status == OCI_SUCCESS) {
        //       /* Retrieve the data type attribute */
        //       checkerr(con->errhp, OCIAttrGet((void*) mypard, (ub4) OCI_DTYPE_PARAM,
        //               (void*) &dtype,(ub4 *) 0, (ub4) OCI_ATTR_DATA_TYPE,
        //               (OCIError *) con->errhp  ));
        //       /* Retrieve the column name attribute */
        //       col_name_len = 0;
        //       checkerr(con->errhp, OCIAttrGet((void*) mypard, (ub4) OCI_DTYPE_PARAM,
        //               (void**) &col_name, (ub4 *) &col_name_len, (ub4) OCI_ATTR_NAME,
        //               (OCIError *) con->errhp ));
        //       /* Retrieve the length semantics for the column */
        //       char_semantics = 0;
        //       checkerr(con->errhp, OCIAttrGet((void*) mypard, (ub4) OCI_DTYPE_PARAM,
        //               (void*) &char_semantics,(ub4 *) 0, (ub4) OCI_ATTR_CHAR_USED,
        //               (OCIError *) con->errhp  ));
        //       col_width = 0;
        //       if (char_semantics)
        //           /* Retrieve the column width in characters */
        //           checkerr(con->errhp, OCIAttrGet((void*) mypard, (ub4) OCI_DTYPE_PARAM,
        //                   (void*) &col_width, (ub4 *) 0, (ub4) OCI_ATTR_CHAR_SIZE,
        //                   (OCIError *) con->errhp  ));
        //       else
        //           /* Retrieve the column width in bytes */
        //           checkerr(con->errhp, OCIAttrGet((void*) mypard, (ub4) OCI_DTYPE_PARAM,
        //                   (void*) &col_width,(ub4 *) 0, (ub4) OCI_ATTR_DATA_SIZE,
        //                   (OCIError *) con->errhp  ));
        //       /* increment counter and get next descriptor, if there is one */
        //       counter++;
        //       parm_status = OCIParamGet((void *)stmthp, OCI_HTYPE_STMT, con->errhp,
        //              (void **)&mypard, (ub4) counter);
        //    } /* while */


        //    const char* sql="begin :res:=pck_operator.login(:loginname,:pass,:ip); end;";
        //    OCIStmtPrepare (stmthp, con->errhp, (CONST OraText *)sql,
        //                                      (ub4)strlen(sql),
        //                                      OCI_NTV_SYNTAX, OCI_DEFAULT);
        //    OCILobLocator * one_lob;
        //    OCIBind       *bndhp[4];
        //    OCIDescriptorAlloc(con->envhp, (void *)&one_lob, OCI_DTYPE_LOB, 0, NULL);
        //    OCIBindByName(stmthp, &bndhp[0], con->errhp, (text *) ":res",strlen(":res"),(void *) &one_lob, sizeof(OCILobLocator*), SQLT_CLOB, (void *) 0,(ub2 *) 0, (ub2 *) 0, (ub4) 0, (ub4 *) 0, OCI_DEFAULT);
        //    OCIBindByName(stmthp, &bndhp[1], con->errhp, (text *) ":loginname",strlen(":loginname"), (ub1 *)"100000" , strlen("100000")+1, SQLT_STR, (void *) 0,(ub2 *) 0, (ub2 *) 0, (ub4) 0, (ub4 *) 0, OCI_DEFAULT);
        //    OCIBindByName(stmthp, &bndhp[2], con->errhp, (text *) ":pass",strlen(":pass"), (ub1 *)"123456" , strlen("123456")+1, SQLT_STR, (void *) 0,(ub2 *) 0, (ub2 *) 0, (ub4) 0, (ub4 *) 0, OCI_DEFAULT);
        //    OCIBindByName(stmthp, &bndhp[3], con->errhp, (text *) ":ip",strlen(":ip"), (ub1 *)"123456" , strlen("123456")+1, SQLT_STR, (void *) 0,(ub2 *) 0, (ub2 *) 0, (ub4) 0, (ub4 *) 0, OCI_DEFAULT);
        //    sword r=OCIStmtExecute (svchp, stmthp, con->errhp, (ub4)1, (ub4)0, (OCISnapshot *)0, (OCISnapshot *)0, OCI_DEFAULT );

        //    OCILobEnableBuffering(svchp, con->errhp, one_lob);
        //    ub4 offset = 1;
        //    ub1 bufp[32768];
        //    ub4 amtp = 32768;
        //    ub4 nbytes = 32768;
        //    r=OCILobRead(svchp, con->errhp, one_lob, &amtp, (ub4) offset, (dvoid *) bufp,
        //                      (ub4) nbytes, (dvoid *)0,
        //                      (sb4 (*)(dvoid *, CONST dvoid *, ub4, ub1)) 0,
        //                      (ub2) 0, (ub1) SQLCS_IMPLICIT);
        //    chk_err(con->errhp, r, __LINE__, (OraText *)"OCIDateFromText");


  }

    ub4 rsetcnt;
    OCIAttrGet((void *)stmthp, OCI_HTYPE_STMT, &rsetcnt, 0, OCI_ATTR_IMPLICIT_RESULT_COUNT, errhp);
    void *result;

}

static ngx_int_t init_func(ngx_cycle_t *cycle) {
    char buf[2048];
    static PGconn *conn;
    FILE *fn=NULL;
    fn=fopen("oracle.conf","r");
    if (fn==NULL) {fclose(fn) ;return NGX_ERROR;}
    con=malloc(sizeof(struct s_conninfo));
    con->oracle_db=NULL;
    con->oracle_pass=NULL;
    con->oracle_user=NULL;
    con->params=NULL;
    while (!feof(fn)) {
        memset(buf,0,sizeof(buf));
        if (fgets(buf,2048,fn)==NULL) continue;
        if (buf[strlen(buf)-1]=='\n') buf[strlen(buf)-1]=0;
        if (strncmp(buf,"db=",3)==0) {
            if (strlen(buf)==3) continue;
            if (con->oracle_db!=NULL) free(con->oracle_db);
            con->oracle_db=(char *)malloc(strlen(buf)-3);
            strcpy(con->oracle_db,buf+3);
        }
        else if (strncmp(buf,"user=",5)==0) {
            if (strlen(buf)==5) continue;
            if (con->oracle_user!=NULL) free(con->oracle_user);
            con->oracle_user=(char *)malloc(strlen(buf)-5);
            strcpy(con->oracle_user,buf+5);//,strlen(buf)-5);
        }
        else if (strncmp(buf,"pass=",5)==0) {
            if (strlen(buf)==5) continue;
            if (con->oracle_pass!=NULL) free(con->oracle_pass);
            con->oracle_pass=(char *)malloc(strlen(buf)-5);
            strcpy(con->oracle_pass,buf+5);
        }
        else if (strncmp(buf,"maxconn=",8)==0) {
            if (strlen(buf)==8) continue;
            MAX_CONN=atoi(buf+8);
//            if (MAX_CONN==NULL) MAX_CONN=100;
        }
    }
    con->timeout=1;
    fclose(fn);
    if (con->oracle_db==NULL||con->oracle_user==NULL||con->oracle_pass==NULL) return NGX_ERROR;
/*    if (!OCI_Initialize(err_handler, NULL, OCI_ENV_DEFAULT | OCI_ENV_THREADED))
    {
        return NGX_ERROR;
    }
    pool = OCI_PoolCreate(oracle_db, oracle_user, oracle_pass, OCI_POOL_SESSION, OCI_SESSION_DEFAULT, 0, MAX_CONN, 1);
    OCI_Connection *cn = OCI_PoolGetConnection(pool, NULL);
    if(cn)
      {
        printf(" the current busy cnt : %u,%u,%u,%s,%s,%s,%s",OCI_PoolGetBusyCount(pool),OCI_PoolGetOpenedCount(pool),OCI_PoolGetMax(pool),OCI_GetDatabase(cn),
        OCI_GetInstanceName(cn),OCI_GetServiceName(cn),OCI_GetServerName(cn));
      }
    OCI_Lob * ires;
    OCI_Statement *st = OCI_StatementCreate(cn);
    OCI_Prepare(st, "begin :res := operator.login1(:loginname,:pass,:ip); end;");
    OCI_BindString(st, ":loginname", "100000",6);
    OCI_BindString(st, ":pass", "123456",6);
    OCI_BindLob(st,":res",ires);
    OCI_Execute(st);
*/
    /* Create a thread-safe OCI environment with N' substitution turned on.  */
    if(OCIEnvCreate((OCIEnv **)&(con->envhp), (ub4)OCI_THREADED ,(void  *)0, 0, 0, 0, (size_t) 0, (void  **)0))
    {
        printf("Failed: OCIEnvCreate()\n");
        return    NGX_ERROR;
    }
    sword r;
    OCIHandleAlloc((dvoid *)con->envhp, (dvoid **)&con->poolhp, OCI_HTYPE_SPOOL, (size_t) 0, (void **) 0);
    OCIError *errhp;
    OCIHandleAlloc((dvoid *)con->envhp, (dvoid **)&errhp, OCI_HTYPE_ERROR,0,(void **)0);
    OCISessionPoolCreate(con->envhp, errhp,  con->poolhp,
                         (OraText **)&con->poolName,
                         (ub4 *)&con->poolNameLen,
                         (OraText *) con->oracle_db, (sb4) strlen(con->oracle_db),
                         (ub4) 1, (ub4) MAX_CONN,
                         (ub4) 1, (OraText *) con->oracle_user,
                         (sb4) strlen(con->oracle_user), (OraText *) con->oracle_pass,
                         (sb4) strlen(con->oracle_pass),  (ub4) OCI_DEFAULT);
    OCIAttrSet((dvoid *) con->poolhp,
               (ub4) OCI_HTYPE_SPOOL, (dvoid *) &con->timeout, (ub4)0,
               OCI_ATTR_SPOOL_TIMEOUT, errhp);
    OCIHandleAlloc((dvoid *) con->envhp, (dvoid **)&con->authp, (ub4) OCI_HTYPE_AUTHINFO, (size_t) 0, (dvoid **) 0);
    OCIAttrSet((dvoid *) con->authp,(ub4) OCI_HTYPE_AUTHINFO,
               (dvoid *) con->oracle_user, (ub4) strlen(con->oracle_user),
               (ub4) OCI_ATTR_USERNAME, errhp);
    OCIAttrSet((dvoid *) con->authp,(ub4) OCI_HTYPE_AUTHINFO,
               (dvoid *) con->oracle_pass, (ub4) strlen(con->oracle_pass),
               (ub4) OCI_ATTR_PASSWORD, errhp);
//    OCISvcCtx *svchp = (OCISvcCtx *) 0;
    OCISessionGet(con->envhp, errhp, &con->svchp, con->authp,
                   (OraText *)con->poolName, (ub4)strlen((char *)con->poolName), NULL,
                   0, NULL, NULL, NULL, OCI_SESSGET_SPOOL);
    OCIStmt *stmthp = (OCIStmt *)0;
    OCIHandleAlloc(con->envhp, (dvoid**)&stmthp,
            OCI_HTYPE_STMT, 0, (dvoid **)0);
    const char* sql1="select count(*) from all_arguments where owner='PDBADMIN' and position=1 and package_name is not null";
    r=OCIStmtPrepare (stmthp, errhp, (CONST OraText *)sql1,
                                      (ub4)strlen(sql1),
                                      OCI_NTV_SYNTAX, OCI_DEFAULT);
    OCIDefine *m_pOCIDefSelect = NULL;
    r=OCIDefineByPos(stmthp, &m_pOCIDefSelect, errhp, 1, (dvoid *) &storednum, (sword) sizeof(storednum), SQLT_INT, (dvoid *) 0, (ub2 *)0, (ub2 *)0, OCI_DEFAULT);
    r=OCIStmtExecute (con->svchp, stmthp, errhp, (ub4)1, (ub4)0, (OCISnapshot *)0, (OCISnapshot *)0, OCI_DEFAULT );
    checkerr(errhp, r,"r:");
    OCISessionRelease(con->svchp,errhp,(OraText *)0,0,OCI_DEFAULT);
    if (con->params!=NULL) {
        for (unsigned int i=0;i<storednum;i++) {
            free(con->params[i].storedprocname);
            free(con->params[i].command);
            free(con->params[i].paramnames);
            for (int j=0;j<con->params[i].nParams;j++) {
                free(con->params[i].paramnames);
            }
        }
        free(con->params);
    }

    PGresult *res;
    con->params=(struct s_param *)malloc(sizeof(struct s_param)*storednum);
    const char* sql="select t1.c,t1.dtype||',',t1.command,'\'||substr(t1.package_name,1,length(t1.package_name)-2)||'\'||t1.object_name param,data_type from (select package_name,object_name,max(position) c,'begin :res='||lower(package_name)||'.'||lower(object_name)||'(:'||listagg(lower(substr(argument_name,3,length(argument_name)-2)),',:') within group (order by position)||'); end;' command,listagg(lower(substr(argument_name,3,length(argument_name)-2))||':'||data_type,',') within group (order by position) dtype from all_arguments where owner='PDBADMIN' and position>0 and package_name is not null group by package_name,object_name) t1 left join all_arguments t2 on t1.package_name=t2.package_name and t1.object_name=t2.object_name and t2.position=0 order by param";
    excesql(sql);

    r=OCIStmtPrepare (stmthp, errhp, (CONST OraText *)sql1,
                                      (ub4)strlen(sql1),
                                      OCI_NTV_SYNTAX, OCI_DEFAULT);
    r=OCIDefineByPos(stmthp, &m_pOCIDefSelect, errhp, 1, (dvoid *) &storednum, (sword) sizeof(storednum), SQLT_INT, (dvoid *) 0, (ub2 *)0, (ub2 *)0, OCI_DEFAULT);
    r=OCIStmtExecute (con->svchp, stmthp, errhp, (ub4)1, (ub4)0, (OCISnapshot *)0, (OCISnapshot *)0, OCI_DEFAULT );
    checkerr(errhp, r,"r:");

/*    for (unsigned int i=0;i<storednum;i++) {
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
    }*/
    PQclear(res);
    freeconn(conn);
    return NGX_OK;
}

static ngx_int_t ngx_http_procoracle_handler(ngx_http_request_t *req) {
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

static ngx_int_t ngx_http_procoraclejson_handler(ngx_http_request_t *req) {
    return NULL;
};

static ngx_int_t ngx_http_queryoracle_handler(ngx_http_request_t *req) {
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

static char *ngx_http_procoracle(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  ngx_http_core_loc_conf_t *corecf;
  corecf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
  corecf->handler = ngx_http_procoracle_handler;
  return NGX_CONF_OK;
}

static char *ngx_http_procoraclejson(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  ngx_http_core_loc_conf_t *corecf;
  corecf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
  corecf->handler = ngx_http_procoraclejson_handler;
  return NGX_CONF_OK;
}

static char *ngx_http_queryoracle(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
  ngx_http_core_loc_conf_t *corecf;
  corecf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
  corecf->handler = ngx_http_queryoracle_handler;
  return NGX_CONF_OK;
}

static ngx_command_t oracle_commands[] = {
    {
        ngx_string("procoracle"),
        NGX_HTTP_LOC_CONF | NGX_CONF_ANY,
        ngx_http_procoracle,
        NGX_HTTP_LOC_CONF_OFFSET,
        0,
        NULL
    },
    {
        ngx_string("procoraclejson"),
        NGX_HTTP_LOC_CONF | NGX_CONF_ANY,
        ngx_http_procoraclejson,
        NGX_HTTP_LOC_CONF_OFFSET,
        0,
        NULL
    },
    {
        ngx_string("queryoracle"),
        NGX_HTTP_LOC_CONF | NGX_CONF_ANY,
        ngx_http_queryoracle,
        NGX_HTTP_LOC_CONF_OFFSET,
        0,
        NULL
    },
    ngx_null_command
};

static ngx_http_module_t ngx_http_oracle_module_ctx = {
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
};

ngx_module_t ngx_http_oracle_module = {
  NGX_MODULE_V1,
  &ngx_http_oracle_module_ctx,
  oracle_commands,
  NGX_HTTP_MODULE,
  NULL, init_func ,NULL,  NULL, NULL, NULL, NULL,
  NGX_MODULE_V1_PADDING
};



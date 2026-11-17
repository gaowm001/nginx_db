# NGXDB 开发参考（代码模板与文件地图）

与 SKILL.md 配套使用。以下模板均来自本项目已验证的实现。

## 1. 手工开发 CRUD 骨架（以 hr.emp 为例）

```sql
CREATE SCHEMA IF NOT EXISTS hr;

CREATE TABLE IF NOT EXISTS hr.emp(
  id bigint PRIMARY KEY,
  empno varchar(32) NOT NULL,
  empname varchar(64) NOT NULL,
  deptid bigint,
  hiredate date,
  isused smallint DEFAULT 1,         -- 1 启用 0 停用（逻辑删除统一用 isused，del/undel 切换）
  updatetime timestamp DEFAULT now()
);
ALTER TABLE hr.emp OWNER TO gm;
COMMENT ON TABLE hr.emp IS '人事-员工档案';

CREATE OR REPLACE FUNCTION hr.emp_query(p_token varchar, p_rows int, p_page int, p_key varchar)
RETURNS json LANGUAGE plpgsql SECURITY DEFINER AS $$
declare v_check json; v_rows json; v_total bigint;
begin
  v_check:=gm.check_login(p_token, 9201);                 -- ① 鉴权：权限点见 §2
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  select count(*) into v_total from hr.emp
   where isused=1 and (p_key is null or position(p_key in empname)>0 or position(p_key in empno)>0);
  select array_to_json(array_agg(row_to_json(t))) into v_rows
    from (select * from hr.emp
           where isused=1 and (p_key is null or position(p_key in empname)>0 or position(p_key in empno)>0)
           order by id desc limit greatest(coalesce(p_rows,20),1)
           offset greatest(coalesce(p_page,1)-1,0)*greatest(coalesce(p_rows,20),1)) t;
  return gm.returnjson(0, json_build_object('total',v_total,'rows',coalesce(v_rows,'[]'::json)));
end $$;
ALTER FUNCTION hr.emp_query(varchar,int,int,varchar) OWNER TO gm;
COMMENT ON FUNCTION hr.emp_query(varchar,int,int,varchar) IS '查询员工档案
key:姓名/工号模糊
返回:{"total":总记录数,"rows":[...]}';

CREATE OR REPLACE FUNCTION hr.emp_add(p_token varchar, p_empno varchar, p_empname varchar)
RETURNS json LANGUAGE plpgsql SECURITY DEFINER AS $$
declare v_check json; v_c int; v_n int;
begin
  v_check:=gm.check_login(p_token, 9211);                 -- ② 写操作用各自权限点
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  select count(*) into v_c from hr.emp where empno=p_empno;
  if v_c>0 then return gm.returnjson(100022); end if;     -- ③ 业务校验：编号重复
  insert into hr.emp(empno,empname) values(p_empno,p_empname) returning id into v_n;
  return gm.returnjson(0, json_build_object('id',v_n));
end $$;
ALTER FUNCTION hr.emp_add(varchar,varchar,varchar) OWNER TO gm;
COMMENT ON FUNCTION hr.emp_add(varchar,varchar,varchar) IS '新增员工
empno:工号 empname:姓名
返回:{"errorcode":0,"info":{"id":新id}}';
```

- 分页/过滤/逻辑删除的写法与生成器保持一致：参数 `p_rows`/`p_page`（rows 空返回
  全量）、文本过滤用 `position()`、逻辑删除统一 `isused`（del→0，undel→1）。
- 写操作（add/edit/del）同理：入口鉴权 → 业务校验（重复、状态、余额/库存等，返回
  对应错误码）→ `INSERT/UPDATE/DELETE` → `gm.returnjson(0)`。每个函数都要写
  `COMMENT ON FUNCTION`（接口文档页直接展示）。
- 口令类字段直接写明文即可——`sysinfo.hash_pass_trigger` 会自动转为 blowfish 哈希。
- 树形表（self-referencing upid）必须带循环定义防护：改上级前检查 `p_upid=p_pk`
  与“新上级的 idpath 是否包含自身”，否则返回 100017（参考 `sysinfo.sysorg_edit`）；
  删除有下级的节点返回 100016。

## 2. 权限节点模板（挂在根 100 之下）

```sql
INSERT INTO sysinfo.sysaction (isused,idpath,idlevel,idcount,upid,params,systemid,sysactionid,sysactionname,actionid,isdefault)
VALUES (1,'100.9200',2,0,100,NULL,100,9200,'人事管理',NULL,NULL)
ON CONFLICT (sysactionid) DO UPDATE SET isused=1, upid=EXCLUDED.upid, idpath=EXCLUDED.idpath,
  idlevel=EXCLUDED.idlevel, idcount=EXCLUDED.idcount, sysactionname=EXCLUDED.sysactionname, actionid=EXCLUDED.actionid;

INSERT INTO sysinfo.sysaction (isused,idpath,idlevel,idcount,upid,params,systemid,sysactionid,sysactionname,actionid,isdefault)
VALUES (1,'100.9200.9201',3,0,9200,NULL,100,9201,'员工档案',NULL,NULL),
       (1,'100.9200.9201.9211',4,0,9201,NULL,100,9211,'新增员工',9211,NULL),
       (1,'100.9200.9201.9212',4,0,9201,NULL,100,9212,'修改员工',9212,NULL),
       (1,'100.9200.9201.9213',4,0,9201,NULL,100,9213,'删除员工',9213,NULL)
ON CONFLICT (sysactionid) DO UPDATE SET isused=1, upid=EXCLUDED.upid, idpath=EXCLUDED.idpath,
  idlevel=EXCLUDED.idlevel, idcount=EXCLUDED.idcount, sysactionname=EXCLUDED.sysactionname, actionid=EXCLUDED.actionid;
```

- 函数内 `check_login` 的权限点必须与 `actionid` 一致：query 用菜单节点号（9201），
  add/edit/del 分别用 9211/9212/9213。
- 给具体员工授权走"员工权限"页面（operinfo 模块）；admin 经根 100 自动全权。
- 财务、ERP 另起号段（如 93xx、94xx）。**编号一经使用不要复用/改动。**

## 3. 前端声明（modules.js）

在 `html/assets/modules.js` 仿照现有行添加 `P()` 声明（菜单号、权限点、base、列、
表单字段、操作权限），随后 `python3 tools/gen_module.py --def ...` 也可自动生成。
页面默认获得：列表、模糊搜索、分页、弹窗表单、删除/恢复、导出；树形模块自动获得
"资源管理器"两栏布局。特殊交互页面再在 `app.js` 加路由特判（参考 renderGetHelp）。

## 4. 上线部署

```bash
# Linux
python3 tools/gen_module.py --def tools/my.json --apply --deploy
curl -sk "https://127.0.0.1/func/_reloadfunctions?token=<postgresreloadtoken>"   # 热加载函数清单，免重启
# Windows（管理员 PowerShell）
powershell -NoProfile -ExecutionPolicy Bypass -File tools\secure_deploy.ps1
```

- 热加载函数清单（生成/修改函数后免重启生效，不中断在线请求）：
  `curl -sk "https://127.0.0.1/func/_reloadfunctions?token=<postgresreloadtoken 的值>"`
- 连接池热管理（查状态/热调池大小/开关复用/按入口重载）：
  `curl -sk "https://127.0.0.1/func/_poolctl?token=<postgresreloadtoken 的值>"`

- 部署目录：Linux `/opt/nginx/html/ngxdb`；Windows `C:\nginx\html\ngxdb`
  （环境变量 `NGXDB_DEPLOY_DIR` 覆盖）。
- 全量回归：`bash test/run_all.sh`（Linux）/ `test/run_all.ps1`（Windows）；
  单层运行：`python3 test/test_ngxdb_db.py --password ...`、
  `python3 test/test_ngxdb_web.py --base https://127.0.0.1`。

## 5. 文件地图

| 路径 | 作用 |
|---|---|
| `extension/ngxdb_postgresql.sql` | 完整库脚本（唯一事实来源，手工变更必须回写） |
| `html/index.html` | 前端入口（资源版本号 `?v=N`，改动后 +1） |
| `html/assets/modules.js` | 模块声明（菜单/权限/列/表单） |
| `html/assets/app.js` | 通用渲染 + 特殊页路由 |
| `html/assets/core.js` | api()/token/登录跳转 |
| `tools/gen_module.py` | 模块生成器（--def/--apply/--apply-tx/--deploy/--print-sql） |
| `tools/examples/*.json` | 生成器示例定义（notice/task=单表、parent_child=父子表、region=树表） |
| `tools/secure_deploy.sh|.ps1` | 等保部署：TLS/限流/conn 口令轮换/备份任务 |
| `tools/backup.sh|.ps1` | 每日 pg_dump 备份（保留 14 份） |
| `test/run_all.sh|.ps1`、`test/test_ngxdb_{db,web}.py` | 全量回归（先重建隔离测试库 ngxdb_test） |
| `test/TEST_PLAN.md` | 测试场景（A..N）与函数覆盖矩阵（82/82） |
| `README.md` | 项目亮点、架构、快速开始、核心功能、文档导航 |
| `docs/INSTALL.md` | 环境要求、手动分步安装、安装与启动 FAQ |
| `docs/DEVELOPMENT.md` | 运行原理、接口约定、二次开发指南、开发注意事项 |
| `docs/PERFORMANCE.md` | 安全与性能实测、Tomcat 对比、高并发与海量数据方案 |
| `docs/SECURITY.md` | 等保 2.0 合规检查、整改记录、自查命令 |
| `docs/WINDOWS.md` | Windows/PowerShell 工具链与限制 |

## 6. 错误码速查（returnjson 的 message 来自 sysinfo.errorcode）

| 码 | 含义 |
|---|---|
| 0 | 成功 |
| 100001 | 帐号密码错误 |
| 100005 | 非法登录（无 token） |
| 100006 | 登录已失效（token 过期） |
| 100008 | 无此权限 |
| 100009/100010 | 账号/密码等参数为空 |
| 100012 | 子系统信息出错（token 解析失败/伪造） |
| 100014/100015/100016/100017 | 上级不存在/顶级不能删/有下级不能删/循环定义（树形） |
| 100018 | 记录不存在 |
| 100035 | 口令复杂度不足 |

新增错误码：先查 `sysinfo.errorcode` 选未占用号（100019/100021 已被占用，勿再用），
INSERT 后即可在 `gm.returnjson(码)` 中使用。

## 7. 等保合规检查清单（自查命令）

| 控制项 | 实现 | 自查方法（期望结果） |
|---|---|---|
| 口令存储 | `sysinfo.hash_pass_trigger` blowfish | `select count(*) from sysinfo.operinfo where pass is not null and pass<>'' and left(pass,3) not in ('$2a','$2b','$2y');` → 0 |
| 登录日志不含口令 | loginlog（public）无 pass 列 | `select count(*) from information_schema.columns where table_schema='public' and table_name='loginlog' and column_name='pass';` → 0 |
| 操作审计 | 7 张核心表挂 `gm.audit_trigger` 写 `public.serverlog`（pckname='audit'） | `select count(*) from pg_trigger where tgname='trg_audit' and not tgisinternal;` → 7；`select count(*) from public.serverlog where pckname='audit';` > 0 |
| 请求日志 | postgres 模块每次调用写 `public.serverlog`（含 clientip/operatorid） | `select funcname,logtime from public.serverlog order by logid desc limit 1;` → 最近有记录 |
| 日志归拢 public | 日志只存 public：serverlog（请求+审计）、loginlog（登录）；sysinfo 旧表不再写入 | `select count(*) from sysinfo.loginlog;` 不再增长（历史只读）；登录后 `select max(logid) from public.loginlog;` 递增 |
| 最小权限连接 | 连接账号 `conn`：仅函数 EXECUTE + `public.serverlog`/`public.loginlog` INSERT/序列，无业务表读写；`ngxapp` 已删除 | `select exists(select 1 from pg_roles where rolname='ngxapp');` → f；`select has_table_privilege('conn','public.serverlog','INSERT');` → t |
| 暴露面一致 | 模块加载清单 = `help_query` = 70 个业务函数（排除 pg_catalog/information_schema/private/cron/myself/crypto/gm，仅 prokind='f'） | `grep -a 'find .* functions' /opt/nginx/logs/error.log \| tail -1` → 70；`help_query` 的 total 同值 |
| 传输加密 | TLSv1.2/1.3，HTTP 301 跳转 | `curl -sI http://127.0.0.1/ \| head -1` → 301；`curl -skI https://127.0.0.1/ \| head -1` → 200 |
| 安全头/限流 | X-Frame-Options/nosniff/Referrer-Policy、limit_req 50r/s burst=100、limit_conn 20、server_tokens off | `curl -skI https://127.0.0.1/ngxdb/login.html` 逐项核对响应头 |
| 数据备份 | `/etc/cron.d/ngxdb-backup` 每日 2 点 `pg_dump -Fc`，保留 14 份 | `cat /etc/cron.d/ngxdb-backup`；`ls /opt/ngxdb-backup` |
| 口令复杂度 | 改密/设密校验，错误码 100035 | 弱口令改密应返回 100035 |

> 管理层面（双因素、三权分立、CA 证书、异地备份演练、日志留存 ≥6 个月、告警接入）
> 属制度/环境事项，见 docs/SECURITY.md 1.4；代码层不得破坏上表任何一项。

# NGXDB 开发手册

本手册由 README.md 拆分而来（2026-11-13），内容随主 README 演进同步维护。

## 一、使用说明与接口约定

### 1.1 登录
- 访问：`http://<服务器IP>/login.html`
- 默认账号：`admin`  密码：`123456`
- 登录后进入控制台（`index.html`）：左侧为三大分组菜单，右侧为数据表格（查询/新增/修改/删除/恢复/详情）。

### 1.2 接口调用约定
- 基础路径：`/func/`
- 调用方式：`POST`，`application/x-www-form-urlencoded`
- 命名：`/func/<base>_<op>`，`base` 为模块表名（如 `sysinfo/operinfo`），`op` 为操作（`query`/`add`/`edit`/`merge`/`del`/`undel`）
- 参数：对应存储过程参数名去掉 `p_` 前缀；鉴权需带 `token`
- 返回：JSON，`errorcode=0` 为成功，`rows`/`total` 为数据，`message` 为错误信息

示例：
```bash
# 登录
curl -s -X POST http://HOST/func/sysinfo/login -d 'loginname=admin&pass=123456&system=100'
# 查询员工（token 用登录返回的 token）
curl -s -X POST http://HOST/func/sysinfo/operinfo_query -d "token=<TOKEN>&page=1&rows=20"
# 接口文档（权限 9113；返回当前库全部函数的元数据）
curl -s -X POST http://HOST/func/sysinfo/help_query -d "token=<TOKEN>"
```

### 1.3 运行期配置与热重载

`location /func` 内可用指令（与 `postgresconn` 配在同一 location）：

| 指令 | 说明 |
|---|---|
| `postgresconn "<libpq conninfo>"` | 数据库连接串（必配；命中即注册 `/func` 处理器） |
| `postgrespoolsize N` | 每 worker 连接池空闲上限，默认 16，`0` 禁用池（退回每请求建连） |
| `postgresreloadtoken <token>` | 热重载管理口令；未配置时仅允许 127.0.0.1 触发 |

**不重启 nginx 热加载函数清单**（新增/修改存储过程后执行；其余 worker 在下一个请求进入时自动跟上）：

```bash
curl -s "https://127.0.0.1/func/_reloadfunctions?token=<postgresreloadtoken 的值>"
# 返回 {"errorcode":0,"message":"functions reloaded","functions":70}
```

说明：
- 模块启动时把 `pg_proc` 中业务 schema（已排除 crypto/gm 等内部 schema）的函数清单
  加载进内存二分查找表；热重载即重新执行该加载 SQL 并替换全局函数表。
- 连接池归还连接时会自动 `ROLLBACK` 失败事务并重置 `app.operatorid`，
  业务函数不要依赖跨请求残留的会话级状态。
- `nginx -s reload`（SIGHUP）也会重建 worker 并重新加载函数清单，但会中断在途请求；
  热重载接口不影响在线流量。

**连接池热管理 `/_poolctl`**（按入口隔离的函数表/连接池；鉴权同热重载）：

```bash
# 查看所有入口：池目标/空闲/借出/命中/新建/失败/丢弃统计（password 已脱敏）
curl -sk "https://127.0.0.1/func/_poolctl?token=<token>"
# 根据使用情况热调某入口的空闲连接目标（conn=入口序号或连接串子串）
curl -sk -X POST "https://127.0.0.1/func/_poolctl?action=resize&conn=0&target=32&token=<token>"
# 关闭/恢复某入口的连接复用（off 排空：借出连接用完即关，不再复用）
curl -sk -X POST "https://127.0.0.1/func/_poolctl?action=off&conn=0&token=<token>"
curl -sk -X POST "https://127.0.0.1/func/_poolctl?action=on&conn=0&token=<token>"
# 只重载某一个入口的函数清单（多库时不必全量重载）
curl -sk -X POST "https://127.0.0.1/func/_poolctl?action=reload&conn=0&token=<token>"
```

动态池语义：`target` 是“空闲连接保留目标”，高峰期池空即新建（借出数不受限），
低谷归还时超出 target 的连接自动关闭——池随负载自然伸缩；`target=0` 即关闭复用。
建议依据 `/_poolctl` 返回的 `hit/new/drop/inuse` 判断：`new` 持续偏高→调大 target；
`drop` 持续偏高→可调小 target 省资源。

**gm schema：内部函数专区。** 不对外暴露的函数统一放 `gm`：token 加解密
（`aes_encrypt/aes_decrypt`）、统一鉴权（`check_login`）、统一返回（`returnjson`）、
触发器函数（`hash_pass_trigger`、`audit_trigger`）、安装辅助（`_ensure_roles`）、
服务端 HTTP（`http_get/http_post`）、定时任务（`gettoken/gettokene`）、权限树维护
（`actionupdate`）。nginx 模块加载清单与接口文档均排除 `gm`，新增内部函数请直接
建在 `gm` 下并补 `COMMENT ON FUNCTION`。

**日志表（已统一到 public，按月分区就绪）：**
- `public.serverlog`：nginx postgres 模块每个 HTTP 请求自动写入（clientip/funcname/params/res/operatorid/logtime），模块 C 代码直接 INSERT；`gm.audit_trigger` 操作审计也写此表（pckname='audit' 区分）；
- `public.loginlog`：登录成功/失败日志（不含口令），由 `sysinfo.login`/`loginaccount` 写入。
查询入口：`sysinfo.serverlog_query`（权限 107，查 public.serverlog）与 `sysinfo.loginlog_query`（权限 107，查 public.loginlog，联查操作员姓名）；
海量数据下按 `logtime`/`logintime` 按月分区 + 归档（见 docs/PERFORMANCE.md 2.5）。

### 1.4 权限体系
- `sysinfo.actions`：功能权限权威表（67 行：actionid / actionname / actionurl / params）。
- 登录时返回当前账号可用的 `actions`（actionid 列表），前端据此显隐菜单与操作按钮。
- 授权链：角色(roleinfo) → 角色权限(rolepermission)；员工(operinfo) → 员工权限(operpermission) 关联到 sysaction。
- `operpermission.permissiontype` 语义（写入 `operinfo_*`/`operinfopermission_*` 时必须遵守）：
  **1=员工直接权限**（`sysactionid` 存 `sysaction.sysactionid`）、
  **2=角色权限**（`sysactionid` 存 `roleinfo.roleinfoid`）。
  插入侧按类型分别校验归属（且限当前登录子系统），`sysoper` 由授权结果自动推导。
- **权限子表：JSON 一次性保存**（不另设行级接口）：前端明细把整个子表包装成 JSON，
  传给父表编辑函数 `sysinfo.operinfopermission_edit`（`operatorid`+`operpermissionjson`，
  权限点 139）与 `sysinfo.roleinfo_edit`（`roleinfoid`+`rolepermissionjson`，权限点 132），
  由其在同一事务里按子表主键增删改（先按提交主键差集删除、再按主键更新、最后插入新行），原子更新父子表且子表主键保持稳定；`permissiontype=1` 用树选择器选功能权限，
  `=2` 选角色。查询由 `operinfopermission_query` / `roleinfo_query` 把子表聚合为
  `operpermissionjson` / `rolepermissionjson` 一次性返回。
- **系统内置操作员保护**：`operinfo_del`、`operinfoorg_del`、`operinfopermission_del`
  对 `operatorid in (10000,100000)` 返回 **100036**（系统内置操作员不能删除）。
  新增删除类接口时必须保留该防护。

## 二、系统运行原理

### 2.1 总体链路

本项目是“**nginx + PostgreSQL 函数**”的瘦后端架构：没有 Java/Python/Node 等应用层，
HTTP 请求由 nginx 的 `ngx_http_postgres_module` 直接转发为对 PostgreSQL 函数的调用，
函数返回 JSON，前端单页应用（SPA）负责全部界面渲染。

```
浏览器 (html/index.html + core.js / app.js / modules.js)
   │  POST /func/{schema}/{函数名}   参数为 form 表单
   ▼
nginx (含 ngx_http_postgres_module，配置见 /opt/nginx/conf)
   │  将 HTTP 参数映射为函数入参，调用 PostgreSQL 函数
   ▼
PostgreSQL
   ├─ gm.check_login(token, 权限点)   ← 每个业务函数入口统一鉴权
   ├─ 业务函数（sysinfo.* / gm.* / 未来 hr.*、fin.*、erp.* …）
   └─ gm.returnjson(错误码, 数据)     ← 统一 JSON 包装：{errorcode, message, info}
```

### 2.2 一次典型请求的完整流程（以“查询员工列表”为例）

1. **登录换取 token**：前端调 `sysinfo.login`，校验口令后写 `sysinfo.operinfo.tokenkey`
   与 `tokentime`（有效期），返回 token 与该员工的权限点集合 `actions`。
2. **携带 token 调接口**：前端 `api()` 统一 `POST /func/hr/emp_query`，参数带
   `token=...` 及查询条件。
3. **函数入口鉴权**：`hr.emp_query` 第一行执行
   `v_check:=gm.check_login(p_token, 9201)`——校验 token 存在且未过期（比对
   `tokenkey/tokentime`），再校验权限点 `9201` 是否在该员工的授权链上
   （按 `sysaction.idpath` 前缀匹配，admin 经根节点 100 自动获得全部权限）。
   校验失败直接返回 `100005/100012/100008`，不执行任何业务 SQL。
4. **业务处理**：分页 + 模糊查询（参数化 SQL，禁止拼字符串），结果用
   `gm.returnjson(0, json_build_object('total', n, 'rows', ...))` 返回。
5. **前端渲染**：`app.js` 依 `modules.js` 的模块声明渲染表格/表单/树，错误码非 0
   时按 `errorcode` 表提示。

### 2.3 权限模型（新增模块必须理解）

- `sysinfo.sysaction` 是**权限树**：`systemid` 所属系统、`sysactionid` 节点号、
  `idpath` 全路径（如 `100.9113.224`）、`actionid` 操作点（为空表示菜单/目录节点，
  鉴权时取 `coalesce(actionid, sysactionid)`）。
- **菜单节点**（如 9113 接口文档）控制前端是否显示页面；**操作节点**（如 224 修改
  接口注释）控制某个写操作是否可调用。两者都挂在对应父节点之下。
- 员工授权写入 `sysinfo.operinfo`/关联表，登录时展开成 `actions` 数组下发前端；
  后端函数仍会再次校验（前端隐藏按钮只是体验优化，**不是安全边界**）。

### 2.4 前端机制

- `modules.js` 用 `P()` 声明每个模块：模块 ID、标题、菜单分组、权限点、接口 base、
  列定义、表单字段、操作权限、树形标志等。**大多数 CRUD 页面零定制**，由 `app.js`
  通用渲染（列表 + 搜索 + 分页 + 弹窗表单 + 删除/恢复 + 导出）。
- 少数特殊页面（如接口文档 `#/gethelp`）在 `app.js` 路由中特判渲染。
- 静态资源版本号在 `index.html`（当前 `v=10`）：改动 js/css 后必须 +1，否则浏览器
  缓存旧文件。

### 2.5 函数缓存与发布（重要）

nginx postgres 模块在 **worker 启动时缓存可调用的函数清单**。因此：
- 新增/改名/删除 PostgreSQL 函数后，推荐热重载（不停机）：
  `curl "https://127.0.0.1/func/_reloadfunctions?token=<postgresreloadtoken>"`，
  其余 worker 在下一个请求进入时自动跟上；也可 `sudo systemctl reload nginx`。
- 只改函数体（`CREATE OR REPLACE` 同签名）不影响缓存，可不用重载。

## 三、二次开发指南：新增业务系统（人事 / 财务 / ERP 等）

在本项目下做一套新业务（人事、财务、ERP、CRM…），本质上是重复四件事：
**建表 → 写函数 → 配权限 → 声明前端模块**。数据库即后端，不需要另起应用服务。
本章规范已固化为大模型代码生成 skill：`.claude/skills/ngxdb-development/`
（SKILL.md + reference.md），供 AI 编码助手在本仓库内自动遵循。

### 3.1 总体思路与目录规划

| 步骤 | 产物 | 说明 |
|---|---|---|
| 1. 规划 schema | `CREATE SCHEMA hr;` | 一个业务域一个 schema：hr（人事）、fin（财务）、erp… |
| 2. 建业务表 | `hr.emp` 等表 | `OWNER gm`，主键 `bigint`，建议带逻辑删除/审计字段 |
| 3. 写函数 | `hr.emp_query/add/edit/merge/del/undel` | 入口 `gm.check_login`，出口 `gm.returnjson` |
| 4. 配权限 | `sysinfo.sysaction` 节点 | 菜单节点 + 各操作节点，`INSERT ... ON CONFLICT (sysactionid) DO UPDATE`（重复执行自愈迁移，不残留旧位置孤儿节点） |
| 5. 声明前端 | `html/assets/modules.js` 的 `P()` | 大多数 CRUD 页面零 JS 定制 |
| 6. 部署验证 | psql 应用 + nginx restart + 冒烟 | 见 12.5 上线清单 |

### 3.2 方式一：gen_module.py 一键生成（推荐，覆盖大部分场景）

标准单表、主子表、树形表三类需求**不要手写**，用生成器（工具会同时更新 SQL dump、
modules.js、index.html 版本号、README、TEST_PLAN）：

```bash
cd /path/to/nginx_db

# 1) 参照 tools/examples/ 写一份 JSON 定义（kind: single / parent_child / tree）
cp tools/examples/example_notice.json tools/my_hr_emp.json
#    编辑 my_hr_emp.json：表名、中文名、主键、菜单/操作权限号、字段列表…

# 2) 仅生成文件（不落库），先人工 review 产出的 SQL 与前端配置
python3 tools/gen_module.py --def tools/my_hr_emp.json

# 3) 事务内试跑（执行后回滚，只验证 SQL 能否通过，不污染库）
python3 tools/gen_module.py --def tools/my_hr_emp.json --apply-tx

# 4) 正式应用 + 部署前端
python3 tools/gen_module.py --def tools/my_hr_emp.json --apply --deploy
curl -s "https://127.0.0.1/func/_reloadfunctions?token=<postgresreloadtoken>"  # 热加载，免重启
```

生成器已处理的细节：函数注释（`COMMENT ON FUNCTION`，可在接口文档页在线维护）、
分页/模糊查询、逻辑删除与恢复、树形 `idpath/idlevel/idcount` 递归维护、
树表 `upname` 联查、循环父级与“有子级禁删”校验、树表建表自动内置 id=100 默认根节点
（幂等去重）、权限节点幂等插入。
`parent_child` 以**父表为主模块**（`table`/`fields` 即父表），另用 `child_table`/`child_pk`/
`child_fk`/`child_fields` 声明子表：父表 `_query` 每行返回 `<child>json`（该行全部子表记录
一次性返回），`_add/_edit/_merge` 接收 `p_<child>json json` 并在同一事务里先按父主键删旧
再从 JSON 展开插入（一次性更新父子表），`_del` 带“有子记录禁删”（100016）防护；
**不生成子表独立函数**。同时向 `modules.js` 追加 `SUBTABLECFG.push({...})`，前端父表
“明细”据此把子表整表 JSON 一次性提交给父表 `_edit`。示例见
`tools/examples/example_parent_child.json`。

**基于已有表的父子联动模块**：父表与子表均已存在时，管理台“生成模块”可直接选择
两表并指定子表的**关联字段**（下拉为子表全部列，默认选中与父表主键同名的列）——
生成独立联动模块：函数名以 `父表_子表` 为前缀（`<父表>_<子表>_query/_add/_edit/
_merge/_del`，共 5 个，**不含 undel**，与父/子表各自单独生成的 CRUD 不重名）；
`modules.js` 生成独立模块（模块 id=`父表_子表`）并挂 `SUBTABLECFG`（`editApi`
指向联动 `_edit`）；权限节点照常生成（接口地址以联动前缀命名，按 actionurl 判重）。
不生成任何 CREATE TABLE；子表的 `isused/systemid` 按实际结构条件化写入。

### 3.3 方式二：手工开发（复杂业务，如 ERP 单据联动）

超出“单表 CRUD”的业务（多表事务、审批流、跨域统计）手工写函数，骨架照抄现有模块：

```sql
CREATE SCHEMA IF NOT EXISTS hr;

CREATE TABLE IF NOT EXISTS hr.emp(
  id         bigint PRIMARY KEY,
  empno      varchar(32) NOT NULL,
  empname    varchar(64) NOT NULL,
  deptid     bigint,
  hiredate   date,
  state      smallint DEFAULT 1,          -- 1 在职 0 离职
  isdel      smallint DEFAULT 0,
  updatetime timestamp DEFAULT now()
);
ALTER TABLE hr.emp OWNER TO gm;

COMMENT ON TABLE hr.emp IS '人事-员工档案';

CREATE OR REPLACE FUNCTION hr.emp_query(p_token varchar, p_page int, p_pagesize int, p_key varchar)
RETURNS json LANGUAGE plpgsql SECURITY DEFINER AS $$
declare v_check json; v_rows json; v_total bigint;
begin
  v_check:=gm.check_login(p_token, 9201);                 -- ① 鉴权：权限点见 3.4
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  select count(*) into v_total from hr.emp
   where isdel=0 and (p_key is null or empname ilike '%'||p_key||'%' or empno ilike '%'||p_key||'%');
  select array_to_json(array_agg(row_to_json(t))) into v_rows
    from (select * from hr.emp
           where isdel=0 and (p_key is null or empname ilike '%'||p_key||'%' or empno ilike '%'||p_key||'%')
           order by id desc limit coalesce(p_pagesize,20) offset coalesce(p_page,1)*coalesce(p_pagesize,20)-coalesce(p_pagesize,20)) t;
  return gm.returnjson(0, json_build_object('total',v_total,'rows',coalesce(v_rows,'[]'::json)));
end $$;
ALTER FUNCTION hr.emp_query(varchar,int,int,varchar) OWNER TO gm;
```

写操作（add/edit/del）同理：入口鉴权 → 业务校验（重复、状态、余额/库存等，返回
对应错误码）→ `INSERT/UPDATE/DELETE` → `gm.returnjson(0)`。多表联动的单据（如 ERP
出入库同时扣库存）必须放在**同一个函数体内**完成，依赖 plpgsql 异常自动回滚，
不要让前端分两次调用拼接事务。

### 3.4 权限节点设置（以人事系统为例）

在 `sysinfo.sysaction` 中为人事系统建立自己的菜单与操作节点（编号建议按域分段，
避开已用的 91xx/224 等）：

```sql
-- 菜单目录：人事管理（挂在根 100 之下）
INSERT INTO sysinfo.sysaction (isused,idpath,idlevel,idcount,upid,params,systemid,sysactionid,sysactionname,actionid,isdefault)
VALUES (1,'100.9200',2,0,100,NULL,100,9200,'人事管理',NULL,NULL)
ON CONFLICT (sysactionid) DO UPDATE SET isused=1, upid=EXCLUDED.upid, idpath=EXCLUDED.idpath,
  idlevel=EXCLUDED.idlevel, idcount=EXCLUDED.idcount, sysactionname=EXCLUDED.sysactionname, actionid=EXCLUDED.actionid;
-- 菜单：员工档案 + 六个操作点（query 沿用菜单号，写操作各占一号）
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
- 给具体员工授权：走“员工权限”页面（operinfo 模块）勾选；admin 无需配置（根 100 自动全权）。
- 财务、ERP 同理另起号段（如 93xx、94xx），**编号一经使用不要复用/改动**。

### 3.5 上线清单（每个新模块都走一遍）

1. SQL 已同步进 `extension/ngxdb_postgresql.sql`（唯一事实来源，手工开发也要回写 dump）。
2. `--apply`（或 psql 执行）到在用库；`sysaction` 插入用了 `ON CONFLICT (sysactionid) DO UPDATE`（幂等且自愈迁移），可重复执行。
3. `--deploy` 或手动拷贝 `html/` 到 `/opt/nginx/html/ngxdb`，`index.html` 版本号 +1。
4. 热加载函数清单：`curl "https://127.0.0.1/func/_reloadfunctions?token=<postgresreloadtoken>"`（新函数必须重载后才能经 HTTP 访问）。
5. 冒烟：无 token→100005；伪 token→100012；admin 带 token→0；写操作各自权限点生效。
6. 按 `test/TEST_PLAN.md` 模板补充并执行该模块的测试场景。

### 3.6 管理台建模（admin，推荐）

无需手写 JSON/SQL：打开本地管理台（`http://127.0.0.1:8765/`），
“数据表管理”建表（结构列可选）→“模块生成”选表、在权限树选挂载点、勾字段 → 生成 →
发布，即自动完成本节全部步骤（函数/权限节点/actions/modules.js/热重载）。
细节见 `admin/README.md`；手工方式（下述）适合批量或需要版本化评审的场景。

## 四、开发注意事项（重要）

1. **鉴权不可省略**：每个业务函数第一行必须 `gm.check_login`。前端隐藏菜单/按钮
   只是体验，不是安全边界；没有后端校验的接口等于裸奔。
2. **统一返回结构**：业务函数一律返回 `gm.returnjson(错误码, 数据)`，错误码沿用
   `sysinfo.errorcode` 已有值（100005 未登录、100012 token 无效、100008 无权限、
   100010 参数为空、100014/100016/100017/100018 树与记录类错误）；确需新错误码时
   先在 errorcode 表登记，前端提示文案依赖它。
3. **OWNER 与权限最小化**：业务表/函数 `OWNER gm`；只有像 `help_edit` 这类必须写
   系统目录（`pg_description`）的函数才用 `OWNER postgres`。严禁用超级用户跑业务，
   严禁在函数里 `EXECUTE` 拼接用户输入。
4. **SQL 注入防护**：值一律参数化/绑定；模糊查询注意转义 `%` `_`；标识符
   （表名/列名/排序字段）绝不能来自前端拼接——用白名单映射。
5. **事务边界**：一次 HTTP 调用 = 一个函数 = 一个事务。多表联动放在同一函数内，
   让 plpgsql 异常自动回滚；不要靠前端顺序调用多个接口“拼事务”。
6. **主键与分页**：主键用 `bigint`（PG18 下函数参数也声明 `bigint`，不要用无类型
   字面量重载）；列表查询必须带 `total` + 分页，避免全表拉取。
7. **nginx 函数缓存**：新增/改名/删除函数后执行 `/_reloadfunctions` 热重载（免重启）；
   仅改函数体不用重启。
8. **dump 与在用库同步**：`extension/ngxdb_postgresql.sql` 是唯一事实来源。任何在
   在用库手工执行的 DDL/权限变更，必须回写 dump（或直接用 gen_module.py 生成），
   否则新环境安装会缺对象。
9. **不要改动核心函数签名**：`sysinfo.*`、`gm.*` 已有函数（login/logout、check_login、
   returnjson、changepassword 等）被框架与 nginx 配置依赖；扩展请新增函数，不改旧签名。
10. **前端字段类型**：modules.js 字段类型决定渲染与校验（文本/数字/日期/下拉/多行
    文本等），新增类型先看 `app.js` 的表单渲染是否已支持，避免重复造轮子。
11. **静态资源版本号**：改了 js/css 必须 +1 `index.html` 的 `?v=`，否则用户浏览器
    缓存旧脚本导致“改了没生效”。
12. **先试跑再上线**：SQL 一律先 `--apply-tx`（事务回滚验证）或临时库导入验证，
    再 `--apply`；权限节点插入是幂等的，可放心重复执行。
13. **测试随代码走**：每个新模块在 `test/TEST_PLAN.md` 登记场景并执行；核心链路
    （鉴权失败、分页、增删改查、树/父子特有逻辑）至少各一条。

## 五、分库分表与外键（FDW 方案）

### 5.1 问题

建模生成的函数中，外键处理（写入前校验、query 联查显示字段）默认**直接访问本库的主表**
（`left join 主模式.主表` / `select count(*) from 主表`）。当系统**分库/分表**后，外键的
主表可能位于其它数据库实例，本地 SQL 直接引用会报“relation does not exist”，函数失效。

### 5.2 解决方案：以外部数据访问（postgres_fdw）实现

不改业务 SQL 的写法，把远程主表**映射为本地外部表**，函数仍然“像访问本地表一样”写：

1. `public.fk_meta` 增加 `master_db` 字段（默认 `local`）标记主表所在数据库：
   - `local`：主表在本库，按原方式生成（直接 join/校验）；
   - 其它值（=外部库名）：生成模板把该外键的访问指向本地 `fdwext` 模式下的外部表
     （`fdwext.主表名`），数据经 postgres_fdw 从外部库获取。
2. 管理台「数据表」页的**分库外键(FDW)** 弹窗提供两步操作：
   - **① 建立外部表映射**（`POST /api/fkfdw/setup`）：填写外部库 host/port/dbname/user/password，
     自动 `CREATE EXTENSION postgres_fdw` → 建 server `fkserver` → 建 postgres/gm 的
     user mapping → 把 `fk_meta` 中 `master_db=该库` 的主表 `IMPORT FOREIGN SCHEMA` 到本地
     `fdwext` 模式（幂等，重复执行安全）；
   - **② 批量改写函数**（`POST /api/fkfdw/rewrite`）：扫描 `fk_meta` 中所有 `master_db<>'local'`
     的外键，把相关已发布函数体内的 `主模式.主表` 批量替换为 `fdwext.主表`，
     `CREATE OR REPLACE FUNCTION` 重新发布，并**同步回写** `extension/ngxdb_postgresql.sql`；
   - 之后执行 `/func/_reloadfunctions` 热重载即可。
3. 新建/修改表时外键行可填写“主库”（默认本机）；`/api/fk`、`/api/fk/save` 支持 `master_db`。

### 5.3 注意事项

- **不建数据库级外键**：`fk_meta` 与建模表都**不创建 FOREIGN KEY 约束**（全库无外键约束）；
  引用完整性由生成函数在写入时校验（100039），分库场景经 FDW 校验；
- **性能**：FDW 跨库查询无本地索引下推，过滤/聚合在本地完成，外部表数据量大时较慢；
  建议外键校验与显示仅用于**小体量主表**（如字典、组织、员工），大表考虑冗余字段或同步；
- **多选外键**：显示字段用标量子查询 `string_agg` 聚合，不会展开行；写入校验逐值验证；
- **安全**：外部库口令存于 user mapping（pg_user_mapping，仅超户可读）；
- **gm Python 模式**：plpython3u 生成器暂不支持外键（含分库场景），需要外键时请使用
  默认 plpgsql 模板；
- **fdwext 约定**：外部表统一映射到本地 `fdwext` 模式，表名与远程一致；请勿在业务 SQL
  中直接改写外部表（当前仅外键校验/联查只读访问）。

## 六、错误码编码规范

业务接口统一返回 `{errorcode, message, info}`，`errorcode=0` 表示成功。错误码分段：

- **100 ～ 10000**：系统错误段（预留/系统使用）。例如：100005 非法登录、100006 登录已失效、
  100008 无此权限、100011～100034 各实体“信息出错”、100035 口令复杂度不足、
  100039 关联数据不存在（外键校验失败）、9998 服务器执行出错（nginx 插件层：SQL 异常、
  参数类型不匹配等，插件捕获后统一返回 `{"errorcode":9998,"message":"...","info":null}`，
  HTTP 状态 200、Content-Type application/json）、9997 接口不存在（nginx 插件层：
  请求的函数未注册，HTTP 404 + 同上 JSON 结构）等；
- **100000 起**：业务扩展错误段（按业务模块顺延使用，当前少量预留）。

新增错误码时：系统类放 100～10000 段空位；业务类从 100000 顺延；并登记到
`sysinfo.errorcode`（前端按 errorcode 显示 message 文案）。

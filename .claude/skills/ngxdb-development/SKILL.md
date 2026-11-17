---
name: ngxdb-development
description: 在 NGXDB 项目中新增业务模块（人事/财务/ERP 等单表、父子表、树形表）、修改数据库函数/权限/前端页面或排查接口问题时使用。包含 nginx+PostgreSQL 函数式后端的硬性规范（鉴权/等保/日志/权限语义）、gen_module.py 一键生成流程、连接池与函数热重载运维、上线清单与常见坑。代码模板与文件地图见同目录 reference.md。
---

# NGXDB 开发规范（供大模型生成本项目代码时遵循）

## 1. 项目是什么（30 秒版）

NGXDB 是"**nginx + PostgreSQL 函数**"的瘦后端管理系统：**没有 Java/Python/Node 应用层**。
浏览器（`html/` 零依赖 SPA）→ nginx（`ngx_http_postgres_module` 把 HTTP 参数直接映射为
PG 函数调用）→ PostgreSQL 函数（业务逻辑全部在库内）。

- 唯一事实来源：`extension/ngxdb_postgresql.sql`（schema/82 个函数/19 张表/权限/初始数据）。
  **任何在用库上的变更必须回写此文件**（或由生成器自动写入）。
- 接口约定：`POST /func/<schema>/<func>[_<op>]`（**斜杠分隔**；点号形式 `schema.func` 会被
  模块直接关闭连接，禁止使用），`application/x-www-form-urlencoded`；参数名去掉 `p_` 前缀
  与函数形参匹配。返回统一为 `{"errorcode":0,"message":"...","info":{...}}`。
- 前端：`html/assets/modules.js` 用 `P()` 声明模块（菜单/权限/base/列/表单/操作权限），
  `app.js` 通用渲染 CRUD 页面；特殊页面在 `app.js` 路由特判（如接口文档 `#/gethelp`）。
- 鉴权：登录（`sysinfo/login`）返回 AES 加密 token + 权限点 actions；每个业务函数第一行
  必须执行 `v_check:=gm.check_login(p_token, <权限点>);`，失败直接 `return v_check;`。

## 2. 硬性规则（生成代码必须遵守）

1. **安全**
   - 每个业务函数入口强制 `gm.check_login`；前端隐藏按钮不是安全边界。
   - **内部函数一律放 `gm` schema**（不进接口文档、不被 nginx 路由）：加解密
     （`gm.aes_encrypt/aes_decrypt`）、触发器函数（`gm.hash_pass_trigger`、`gm.audit_trigger`）、
     安装/维护辅助（`gm._ensure_roles`）等。业务 schema（sysinfo/hr/…）只放对外接口函数；
     新增内部辅助函数时必须创建在 `gm` 下，避免暴露面扩大。
   - SQL 一律参数化/绑定，禁止拼接用户输入；模糊查询注意 `%` `_` 转义；表名/列名/排序
     字段不得来自前端（用白名单映射）。
   - 口令：`sysinfo.operinfo.pass` 由触发器 `gm.hash_pass_trigger` 自动 blowfish 哈希，
     比对用 `pass=crypto.crypt(输入,pass)`；**任何代码不得明文存取口令**，登录日志不记口令。
   - 函数 `LANGUAGE plpgsql SECURITY DEFINER`，`OWNER TO gm`；仅写系统目录（如
     pg_description）的函数才 `OWNER TO postgres`。连接层使用最小权限账号 `conn`
     （仅函数 EXECUTE + `public.serverlog` INSERT/序列，无业务表读写；原 `ngxapp` 已删除）。
   - 审计：operinfo/sysaction/roleinfo/systeminfo/operpermission/rolepermission/loginlog
     已挂 `gm.audit_trigger` 自动写 serverlog；新业务表如需审计，为其创建同样的 AFTER 触发器。
2. **返回与错误码**
   - 一律 `gm.returnjson(错误码, json_build_object(...))`；错误码复用现有值：
     100001 帐号密码错误、100005 非法登录、100006 登录已失效、100008 无此权限、
     100009/100010 参数为空、100012 子系统信息出错、100018 记录不存在、100035 口令复杂度不足。
   - 需要新错误码时先 `INSERT INTO sysinfo.errorcode(message,errorcode,primekey,isused,schema)`
     （先查表选未占用号，100019/100021 已被占用），再在函数中使用（message 来自该表）。
3. **表与函数规范**
   - 一个业务域一个 schema（如 hr、fin、erp）；表 `OWNER gm`，主键 `bigint`
     （PG18 下函数参数也声明 `bigint`，不要用无类型字面量重载）。
   - 标准 CRUD 固定六函数：`_query / _add / _edit / _merge / _del / _undel`
     （query 返回 `total`+`rows` 分页；del/undel 为逻辑删除/恢复）。
   - 树形表维护 `idpath/idlevel/idcount`（参考 sysorg 的递归 CTE 重算与
     - 树形表维护 `idpath/idlevel/idcount`（参考 sysorg 的递归 CTE 重算与
       100014/100016/100017 错误码）；父子表做“有子级禁删”校验（100016）。
   - **父子表（parent_child）以父表为主模块**：父表 `_query` 每行返回 `<child>json`
     （子表一次性返回），`_add/_edit/_merge` 接收 `p_<child>json json` 并在同一事务
     先按父主键删旧再插入（一次性更新父子表），`_del` 带“有子记录禁删”防护；
     **不生成子表独立函数**。同时向 `modules.js` 追加 `SUBTABLECFG.push({...})`，
     父表“明细”据此把子表整表 JSON 一次性提交给父表 `_edit`。
     示例：`tools/examples/example_parent_child.json`。
   - **父子表（已有表）联动模块**：父/子表已存在时，管理台选两表+子表关联字段
     （默认与父表主键同名）生成独立联动模块：函数 `<父表>_<子表>_query/add/edit/merge/del`
     （5 个，无 undel，与单表 CRUD 不重名），模块 id=`父表_子表`，同样挂 SUBTABLECFG。
   - **权限子表**不另设行级接口：把整表权限包装成 JSON 传给父表编辑函数
     `operinfopermission_edit` / `roleinfo_edit` 一次性保存（勿绕过 `check_login`）；
     **系统内置操作员 10000/100000 禁止删除（100036）**。
   - 多表联动（如 ERP 单据+库存）必须在**同一个函数**内完成，靠 plpgsql 异常自动回滚；
     禁止前端多次调用拼事务。
   - 不要修改 sysinfo/gm 既有核心函数签名（login、check_login、returnjson 等）；
     扩展一律新增函数。
4. **发布**
   - nginx postgres 模块在 **worker 启动时缓存函数清单**：新增/改名/删除函数后执行热重载
     `curl "https://127.0.0.1/func/_reloadfunctions?token=<postgresreloadtoken>"`
     （不停止 nginx，其余 worker 在下一个请求进入时自动跟上，见 docs/DEVELOPMENT.md 1.3）；
     仅改函数体（同签名 CREATE OR REPLACE）不用重载。
   - 前端 js/css 变更后必须把 `html/index.html` 里的 `?v=N` 版本号 +1。
   - 部署目录：`install.sh` 全新安装部署到 `/opt/nginx/html/`（URL 前缀 `/`）；
     `gen_module.py --deploy` 与 `secure_deploy` 部署到 `/opt/nginx/html/ngxdb`
     （URL 前缀 `/ngxdb/`，与本项目在线环境一致）；Windows `C:\nginx\html\ngxdb`
     （环境变量 `NGXDB_DEPLOY_DIR` 可覆盖）。前端资源用绝对路径引用 API（`/func/`），
     两种位置均可正常工作。

## 3. 新增业务模块的标准流程

### 方式一（首选）：gen_module.py 一键生成

适用：单表（single）、父子表（parent_child）、树形表（tree）。

```bash
cd <项目根>/src/modules/nginx_db          # 即本文件上溯三级所在目录
cp tools/examples/example_notice.json tools/my_module.json   # 参照示例编辑定义
python3 tools/gen_module.py --def tools/my_module.json          # 仅生成文件，先 review
python3 tools/gen_module.py --def tools/my_module.json --apply-tx   # 事务试跑(回滚验证)
python3 tools/gen_module.py --def tools/my_module.json --apply --deploy
curl -s "https://127.0.0.1/func/_reloadfunctions?token=<postgresreloadtoken>"   # 热加载，免重启
```

生成器会自动更新：`extension/ngxdb_postgresql.sql`（建表+六函数+COMMENT+权限节点；
父子表联动模块为 5 函数且不含建表语句）、
`html/assets/modules.js`、`html/index.html` 版本号、`README.md`、`test/TEST_PLAN.md`。

JSON 定义必填项：`kind`(single/parent_child/tree)、`table`、`chinese`、`pk`、
`menu_action`、`actions`(含 add/edit/merge/del/undel/query 六个权限号)、`fields`；
parent_child 另需 `parent_table/parent_pk/parent_name`。可选：`schema`(默认 sysinfo)、
`group`(默认 9001)、`icon`、`systemid`(默认 100)。

### 方式二（复杂业务）：手工开发

超出单表 CRUD 的业务（审批流、跨表统计、单据联动）手工写函数。完整 SQL 骨架、
权限节点 INSERT 模板与前端声明方式见 **`reference.md`**。

### 上线与验证清单（每次改动都走）

1. SQL 已同步进 `extension/ngxdb_postgresql.sql`（幂等：权限节点用 ON CONFLICT (sysactionid) DO UPDATE）。
2. 在用库执行（生成器 `--apply` 或 psql）；复杂改动先 `--apply-tx` / 临时库试跑。
3. 前端部署（生成器 `--deploy`），`index.html` 版本号 +1。
4. 热重载函数清单：`curl "https://127.0.0.1/func/_reloadfunctions?token=<postgresreloadtoken>"`（新增/改名/删除函数必须；仅改函数体不用）。
5. 冒烟：无 token→100005；伪 token→100012；admin 带 token→0；写操作权限点各自生效。
6. 在 `test/TEST_PLAN.md` 补充并执行场景；可用
   `bash test/run_all.sh`（Linux）或 `test/run_all.ps1`（Windows）跑全量回归。

## 4. 等保合规要点（GB/T 22239-2019，生成代码必须遵守）

1. **身份鉴别**：口令只经 `sysinfo.hash_pass_trigger` 落库（blowfish），比对用
   `crypto.crypt(输入,pass)`；复杂度 ≥8 位且含字母+数字（错误码 100035）；登录日志
   `loginlog` 不含口令列，任何新表/日志不得记录明文口令。
2. **会话与 token**：token 为两层 AES 加密（系统密钥 + 子系统 prikey），tokentype
   1=单人/2=多人，`check_login` 通过后顺延 `tokentime`；日志/响应不得输出 token 明文。
3. **访问控制**：业务函数入口 `gm.check_login(p_token,权限点)` 必填；权限点与
   `sysaction.actionid` 一致；前端隐藏按钮 ≠ 授权。`operpermission.permissiontype`：
   1=员工直接权限（sysactionid 存 sysaction.sysactionid）、2=角色权限（sysactionid 存
   roleinfo.roleinfoid）；写入权限 JSON 时按类型校验归属并限当前子系统。
   **权限子表**不新增行级接口：把整个子表包装成 JSON 传给父表编辑函数
   `sysinfo.operinfopermission_edit`（权限点 139）与 `sysinfo.roleinfo_edit`（权限点 132）
   一次性保存，查询由 `operinfopermission_query` / `roleinfo_query` 一次性返回子表 JSON；
   勿再另写绕过 `check_login` 的权限写入口。
   **系统内置操作员（operatorid 10000/100000）禁止删除**：删除类函数必须保留
   `if p_operatorid in (10000,100000) then return gm.returnjson(100036);` 防护
   （否则误删后系统无法登录、只能重装）。
4. **安全审计**：核心表由 `gm.audit_trigger` 自动写 `public.serverlog`（pckname='audit'）；
   HTTP 每次调用由 postgres 模块写 `public.serverlog`（clientip/funcname/params/res/operatorid，
   operatorid 来自 check_login 设置的 `app.operatorid`）；登录日志统一写 `public.loginlog`。
   新业务表需要审计时创建同样的 AFTER 触发器；审计写入失败不阻断业务。
   **日志一律入 public**（serverlog/loginlog），不要在业务 schema 再建日志表；
   海量日志按月分区+归档（docs/PERFORMANCE.md 2.5），留存 ≥6 个月。
5. **最小权限**：nginx 连接账号固定 `conn`（无业务表读写）。**禁止**用 postgres/gm 连
   nginx，**禁止**再创建类似 ngxapp 的旁路账号；新业务域 schema 需
   `GRANT USAGE/EXECUTE TO conn` 并回写 `extension/ngxdb_postgresql.sql`。
6. **传输与边界**：强制 HTTPS（TLSv1.2/1.3，HTTP 301），`limit_req 50r/s burst=100`、
   `limit_conn 20`，`server_tokens off`，安全头三件套——统一用模板
   `tools/nginx.conf.template` 部署（`tools/secure_deploy.sh`）。管理接口
   `/_reloadfunctions`、`/_poolctl` 默认仅允许 127.0.0.1 调用（不启用 token，
   方便试用）；需要远程管理时配置 `postgresreloadtoken` 即可启用鉴权。
7. **数据备份**：每日 2 点 `pg_dump -Fc`（保留 14 份，`/etc/cron.d/ngxdb-backup`）；
   恢复 `pg_restore --clean --if-exists`；重要环境另做异地同步与恢复演练。
8. **暴露面一致**：nginx 模块加载的函数清单必须与接口文档 `sysinfo.help_query` 一致——
   均排除 `pg_catalog/information_schema/private/cron/myself/crypto/gm` 且仅普通函数。
   改模块加载 SQL 在 `postgres/ngx_http_postgres_module.c`（count 与明细两处），改完必须
   `sudo bash rebuild_nginx.sh` 重编译并重启；新增/删除 schema 时两处同步。

## 5. 常见坑（出现过真实事故/失败的地方）

- **点号 API 形式**（`/func/schema.func`）会被模块直接关闭连接（无 404）——只用斜杠。
- **改函数不热重载** → HTTP 层仍按旧函数清单工作，报"函数不存在"或行为不变。
  ✅ 现已支持热重载：`curl "https://127.0.0.1/func/_reloadfunctions?token=<postgresreloadtoken>"`
  即刻生效，无需重启（见 docs/DEVELOPMENT.md 1.3）。
- **模块内置连接池**（`postgrespoolsize`，默认 16）：连接归还时会 `ROLLBACK` 失败事务并
  重置 `app.operatorid`——业务函数**不得依赖上一请求残留的会话级状态**；生成的函数应
  自带完整事务语义（函数内 COMMIT/ROLLBACK），不依赖外部连接状态。
- systemd nginx 停不下来（历史游离进程不在 unit cgroup 里，ExecStop 停不掉）：
  `systemctl stop nginx; pkill -x nginx; sleep 1; systemctl reset-failed nginx; systemctl start nginx`。
- **token 里的 `+` 未 urlencode**：curl 用 `-d token=...` 会把 `+` 变空格导致 100012；
  必须用 `--data-urlencode`（前端 fetch 已正确编码）。
- 系统目录表（pg_description 等）**不支持 ON CONFLICT**，且 gm 无权写——需要写系统目录的
  函数用 `OWNER TO postgres`（先 UPDATE 未命中再 INSERT，不要 ON CONFLICT）。
- errorcode 编号密集：新增错误码前先查表选**未占用**的号（曾连踩 100019/100021 已占用）。
- Windows：Python 工具直接可用（`NGXDB_DEPLOY_DIR` 覆盖部署目录）；PowerShell 等价脚本
  `tools/backup.ps1`、`tools/secure_deploy.ps1`、`test/run_all.ps1`；`install.sh` 与
  `rebuild_nginx.sh` 仅限 Linux。

---
name: ngxdb-development
description: 在 NGXDB 项目中新增业务模块（人事/财务/ERP 等单表、父子表、树形表）、修改数据库函数/权限/前端页面或排查接口问题时使用。包含 nginx+PostgreSQL 函数式后端的硬性规范、gen_module.py 一键生成流程、上线清单与常见坑。代码模板与文件地图见同目录 reference.md。
---

# NGXDB 开发规范（供大模型生成本项目代码时遵循）

## 1. 项目是什么（30 秒版）

NGXDB 是"**nginx + PostgreSQL 函数**"的瘦后端管理系统：**没有 Java/Python/Node 应用层**。
浏览器（`html/` 零依赖 SPA）→ nginx（`ngx_http_postgres_module` 把 HTTP 参数直接映射为
PG 函数调用）→ PostgreSQL 函数（业务逻辑全部在库内）。

- 唯一事实来源：`extension/ngxdb_postgresql.sql`（schema/81 个函数/19 张表/权限/初始数据）。
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
   - SQL 一律参数化/绑定，禁止拼接用户输入；模糊查询注意 `%` `_` 转义；表名/列名/排序
     字段不得来自前端（用白名单映射）。
   - 口令：`sysinfo.operinfo.pass` 由触发器 `sysinfo.hash_pass_trigger` 自动 blowfish 哈希，
     比对用 `pass=crypto.crypt(输入,pass)`；**任何代码不得明文存取口令**，登录日志不记口令。
   - 函数 `LANGUAGE plpgsql SECURITY DEFINER`，`OWNER TO gm`；仅写系统目录（如
     pg_description）的函数才 `OWNER TO postgres`。连接层使用最小权限账号 `ngxapp`。
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
     100014/100016/100017 错误码）；父子表做 `upname` 联查与"有子级禁删"校验。
   - 多表联动（如 ERP 单据+库存）必须在**同一个函数**内完成，靠 plpgsql 异常自动回滚；
     禁止前端多次调用拼事务。
   - 不要修改 sysinfo/gm 既有核心函数签名（login、check_login、returnjson 等）；
     扩展一律新增函数。
4. **发布**
   - nginx postgres 模块在 **worker 启动时缓存函数清单**：新增/改名/删除函数后必须重启
     nginx（`sudo systemctl restart nginx`，或 stop+清 pid+start，见 README 9.3）；
     仅改函数体（同签名 CREATE OR REPLACE）不用重启。
   - 前端 js/css 变更后必须把 `html/index.html` 里的 `?v=N` 版本号 +1。
   - 部署目录：Linux `/opt/nginx/html/ngxdb`；Windows `C:\nginx\html\ngxdb`
     （环境变量 `NGXDB_DEPLOY_DIR` 可覆盖）。

## 3. 新增业务模块的标准流程

### 方式一（首选）：gen_module.py 一键生成

适用：单表（single）、父子表（parent_child）、树形表（tree）。

```bash
cd <项目根>/src/modules/nginx_db          # 即本文件上溯三级所在目录
cp tools/examples/example_notice.json tools/my_module.json   # 参照示例编辑定义
python3 tools/gen_module.py --def tools/my_module.json          # 仅生成文件，先 review
python3 tools/gen_module.py --def tools/my_module.json --apply-tx   # 事务试跑(回滚验证)
python3 tools/gen_module.py --def tools/my_module.json --apply --deploy
sudo systemctl restart nginx                                    # 新函数必须重启
```

生成器会自动更新：`extension/ngxdb_postgresql.sql`（建表+六函数+COMMENT+权限节点）、
`html/assets/modules.js`、`html/index.html` 版本号、`README.md`、`test/TEST_PLAN.md`。

JSON 定义必填项：`kind`(single/parent_child/tree)、`table`、`chinese`、`pk`、
`menu_action`、`actions`(含 add/edit/merge/del/undel/query 六个权限号)、`fields`；
parent_child 另需 `parent_table/parent_pk/parent_name`。可选：`schema`(默认 sysinfo)、
`group`(默认 9001)、`icon`、`systemid`(默认 100)。

### 方式二（复杂业务）：手工开发

超出单表 CRUD 的业务（审批流、跨表统计、单据联动）手工写函数。完整 SQL 骨架、
权限节点 INSERT 模板与前端声明方式见 **`reference.md`**。

### 上线与验证清单（每次改动都走）

1. SQL 已同步进 `extension/ngxdb_postgresql.sql`（幂等：权限节点用 ON CONFLICT DO NOTHING）。
2. 在用库执行（生成器 `--apply` 或 psql）；复杂改动先 `--apply-tx` / 临时库试跑。
3. 前端部署（生成器 `--deploy`），`index.html` 版本号 +1。
4. `sudo systemctl restart nginx`（新增/改名/删除函数必须；仅改函数体不用）。
5. 冒烟：无 token→100005；伪 token→100012；admin 带 token→0；写操作权限点各自生效。
6. 在 `test/TEST_PLAN.md` 补充并执行场景；可用
   `bash test/run_all.sh`（Linux）或 `test/run_all.ps1`（Windows）跑全量回归。

## 4. 常见坑（出现过真实事故/失败的地方）

- **点号 API 形式**（`/func/schema.func`）会被模块直接关闭连接（无 404）——只用斜杠。
- **改函数不重启 nginx** → HTTP 层仍按旧函数清单工作，报"函数不存在"或行为不变。
- systemd nginx 停不下来（pid 文件坑）：`systemctl stop nginx; pkill -x nginx;
  rm -f /opt/nginx/logs/nginx.pid; systemctl start nginx`。
- **token 里的 `+` 未 urlencode**：curl 用 `-d token=...` 会把 `+` 变空格导致 100012；
  必须用 `--data-urlencode`（前端 fetch 已正确编码）。
- 系统目录表（pg_description 等）**不支持 ON CONFLICT**，且 gm 无权写——需要写系统目录的
  函数用 `OWNER TO postgres`（先 UPDATE 未命中再 INSERT，不要 ON CONFLICT）。
- errorcode 编号密集：新增错误码前先查表选**未占用**的号（曾连踩 100019/100021 已占用）。
- Windows：Python 工具直接可用（`NGXDB_DEPLOY_DIR` 覆盖部署目录）；PowerShell 等价脚本
  `tools/backup.ps1`、`tools/secure_deploy.ps1`、`test/run_all.ps1`；`install.sh` 与
  `rebuild_nginx.sh` 仅限 Linux。

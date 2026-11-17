# NGXDB 修改记录（CHANGELOG）

收录按日期的修复记录、功能更新与专项排查。README 保留项目介绍、安装使用、
开发指南、安全与性能验证、等保合规等常设内容；本文档只做追加，不改写历史。

---
## 一、修复记录（2026-11-11）

**症状**：登录系统后，多个页面间歇性提示「子系统信息出错（100012）」/「非法登录（100005）」。

**根因**：`postgres/ngx_http_postgres_module.c` 作为 content handler 直接从 `req->connection->buffer` 读取 POST body，未等待请求体读全。当请求头与请求体分属不同读事件时（浏览器 / Python 等常见），处理器读到空 body → `token` 参数丢失 → `gm.check_login` 返回 100005/100012。

**修复**：
1. `postgres/ngx_http_postgres_module.c`：改用 `ngx_http_read_client_request_body`，等请求体读全后再处理（body 从 `req->request_body` 获取）。
2. 重新编译安装：`sudo bash rebuild_nginx.sh`（编译 → 备份旧二进制 → `mv` 原子替换，避免 Text file busy → `nginx -t` → 停止并重启以加载新二进制）。
3. 前端 `assets/core.js` / `login.html`：使用 **POST** 提交，并对鉴权类错误(100005/100006/100011/100012)退避重试。

**验证**：POST 方式连续 65 次请求（5 轮 × 13 模块）失败 0 次。

**回访验证（“省/市”页面无数据问题）**：修复前（11-09 21:51 的 access.log 记录）浏览器对 `province_query`/`city_query` 的访问**全部返回 73 字节错误体**，页面上表现为一直无数据——与“子系统出错”是**同一根因**（POST body 丢失 → token 丢失）。重编译后实测：`province` 正常返回 34 行、`city` 正常返回 360 行，连续 65 次请求 0 失败。若浏览器仍显示无数据，请先 `Ctrl+F5` 强制刷新（资源版本已升至 `?v=5`）再测。

### 关于早期的两处注意事项（现已消除）
1. **口令经 URL 传输**：修复前曾临时把前端改为 GET(query string)，会使登录口令出现在 URL 与 `access.log` 中。**重编译后前端已改回 POST**，口令随请求体传输、不再进入 URL/access.log。
2. **URL 长度限制**：GET 方式下较大的权限 JSON 可能逼近 nginx `large_client_header_buffers`(默认 8k)。**改回 POST 后不再受此限制**。

**相关文件**：`postgres/ngx_http_postgres_module.c`（已修复，备份 `.bak.*`）、`rebuild_nginx.sh`（重编译安装脚本）、`html/assets/core.js`、`html/login.html`、`html/index.html`。

### 补充修复：POST 请求不终结（finalize 缺失，2026-11-12）
**现象**：浏览器 F12 的 Network 面板中**大量 POST 无响应（一直 pending）**，且**连接复用后后续请求的请求体丢失参数（连 token 都没有）**。
**根因**：改用 `ngx_http_read_client_request_body` 异步回调后，回调函数**丢弃了处理函数的返回值、未调用 `ngx_http_finalize_request`**——请求处理完却永不终结：
- keepalive 连接被污染：复用该连接的后续请求挂起（无响应）或 body 解析错乱（参数丢失）；
- `access.log` 完全不记录（铁证：11/12 当天浏览器的所有 POST 均无日志，而 serverlog 里全有）。
**修复**：回调中 `rc=ngx_http_procpostgres_process(req); ngx_http_finalize_request(req, rc);`
**验证**：连发 27 个 POST（登录 1 + 查询 26）→ `access.log` 记录数增量**恰为 27**（修复前缺失），0 失败，总耗时 0.51s。

> 另：serverlog 中成串的 `100006 登录已失效` 属**单人登录设计**（`tokentype=1`，每次新登录使旧会话失效，多浏览器/多标签会互相踢），前端会自动跳回登录页，属正常行为。

### 补充修复：登录态跳转（会话失效 / 退出登录）
- **退出登录不成功**：`core.js` 的 `logout()` 原先带一个“一次性锁”`if(_loggingOut) return; _loggingOut=true;`——只要此前触发过一次 logout（例如某次会话失效），`_loggingOut` 便永远为 true，之后点击“退出登录”会被**静默忽略**。现改为**无锁**并统一用 `location.replace()` 跳转。
- **会话失效返回登录页并可回跳**：`logout()` 跳转时携带 `?redirect=<当前页面>`；`login.html` 登录成功后读取该参数**原路返回**（仅接受同源绝对路径，避免开放重定向），无参数则进入 `index.html`。
- **未登录访问**：`index.html` 初始化检测到无 token 时调用 `logout()`（同样带回跳参数）。
- 资源版本号升到 `?v=4`（现为 `?v=5`）以强制刷新缓存。

### 性能排查：点击侧边导航"响应慢"的原因定位（2026-11-11）
**实测结论：服务器端不是瓶颈。** 多轮压测数据（Linux 本机直连）：
- 串行 3 轮 × 13 个模块：每请求 **10~22ms**，一轮共 0.13~0.17s；
- 并发 13 个请求（模拟快速连点）：总耗时 **0.118s**，单请求最大 0.115s；
- `sysinfo.login`（含 AES 加密）**12ms**；`gm.check_login`（含 AES 解密 + 递归 CTE + update）仅 **2ms**；
- AES 为 pgcrypto（C 实现，非脚本实现）；数据量小（最大 city 360 行，前端每页仅渲染 15 行）。

因此点击导航的耗时几乎全部消耗在**浏览器↔服务器之间（8080 代理链路）**或**浏览器渲染**（VMware 虚拟机为软件渲染，DOM 重建 + 表格绘制较慢）。

已在 `core.js` 的 `api()` 加入**耗时打点**：按 `F12` → Console，每次导航会输出 `[ngxdb] /func/sysinfo/xxx_query → Nms`（该值不含页面渲染时间）：
- 若该值 **>300ms** → 慢在 **8080 代理/网络链路**（请检查 Windows 侧代理：是否启用 upstream keepalive、是否每次请求都解析上游域名等）；
- 若该值 **<50ms** 而页面仍觉得慢 → 慢在**浏览器渲染**（虚拟机软件渲染所致，属正常现象）。

## 二、功能更新：树状展示（2026-11-12）
- **范围**：表结构中带上级字段（`upid`）的模块——**权限管理(sysaction)**、**部门管理(sysorg)**。
- **后端**：`sysaction_query`/`sysorg_query` 返回值新增 `idpath`/`idlevel`，并改按 `idpath` 排序（父先子后的树序，树深度即 `idlevel-1`）。
- **前端**：`renderRows` 对树模块按 `idlevel` 层级缩进展示；有下级的行显示 **▼/▶**，点击可**折叠/展开**子树（按 idpath 前缀匹配隐藏后代）；行操作按钮改用**主键定位**（折叠后索引不错位）。
- **效果**：权限管理页呈现为「系统 → 系统管理 → 系统权限 → 新增/修改/…」的层级树；部门管理随部门数据增加自动成树。
- **涉及文件**：`extension/ngxdb_postgresql.sql`、`html/assets/modules.js`（`P()` 新增 `tree` 参数）、`html/assets/app.js`（`treeDepth`/`renderRows`）、`html/assets/suite.css`（树样式）。

### 2.1 树状模块升级为“资源管理器”两栏布局（2026-11-12）

权限管理、部门管理等树状模块的界面由“整表缩进”升级为模仿 Windows 资源管理器的**左右两栏**：

- **左栏（层级导航）**：完整树。文件夹图标（展开/折叠状态区分）、子项计数徽标、
  ▼/▶ 折叠箭头；点击节点名=在右栏打开该层级；点击箭头=仅展开/折叠，不切换。
  首次进入自动展开第一层。
- **右栏（当前层级列表）**：只显示**当前选中节点的直接下级**（与资源管理器一致），
  顶部有**面包屑**（全部 / 一级 / 二级 …，各级可点击回跳）；选中行后仍可编辑/删除/配权限。
- **搜索**：在右栏搜索框输入时，跨整棵树匹配，命中项连同其各级父节点一起列出，
  面包屑显示“搜索…命中 N 项”。
- 技术说明：后端仍按 `idpath` 排序返回（含 idpath/idlevel 字段）；前端据此构建父子索引，
  右栏用 `idpath === 父id + '.' + 自身id` 过滤直接下级；资源版本升至 `?v=7`。
- 非树模块（系统管理、员工、日志、省市等）保持原有单表格布局，不受影响。

### 2.2 数据库函数缺陷修复（2026-11-12，由测试发现）

测试套件（`test/`）运行暴露 4 个真实 SQL 缺陷，均已修复并同步到在用库（CREATE OR REPLACE）：

| 缺陷 | 涉及函数 | 后果 |
|---|---|---|
| `json_to_recordset(json, boolean)` 两参调用在 PG18 不存在 | operinfo_edit、operinfoorg_edit、operinfopermission_add/edit/merge、roleinfo_edit | 员工权限/角色编辑功能一执行即报错 |
| 权限/系统归属写入误用 `p_operatorid`（自动编号时为 null） | operinfo_add | 新增员工报 non-null 违约，功能不可用 |
| 权限 JSON 过滤条件 `AND` 恒假 | operinfo_add、operinfopermission_add | 权限永远写不进去 |
| 合并新建员工缺 sysoper 归属、delete 条件变量用错 | operinfoorg_merge | 新建员工无法登录/删除 |

修复后回归：**数据库层测试 141 通过 / 0 失败 / 4 已知问题**（已知问题为 dump 缺少 pkg_service、sms 模式及 funchtml 空表，见 `test/TEST_PLAN.md` 第 5 节）。

### 2.3 环境问题：systemd nginx.service 循环重启（重要）

本机存在 `/etc/systemd/system/nginx.service`（enabled），与手动运行中的 `/opt/nginx` nginx **争抢 80 端口**：systemd 服务陷入每数秒一次的重启循环，error.log 被 `bind() to 0.0.0.0:80 failed` 刷屏。该循环会造成**间歇性 POST /func 连接被断开**（表现为页面偶发无响应、测试报 RemoteDisconnected）。

**根治**（需 root，二选一）：
```bash
sudo systemctl disable --now nginx    # 停用 systemd 版，继续用 /opt/nginx 手动实例
# 或：/opt/nginx/sbin/nginx -s stop && sudo systemctl start nginx   # 交给 systemd 托管
```


> **补充（2026-11-12）**：`install.sh` 已内置 **systemd 服务注册**（步骤 4.5：写入
> `/etc/systemd/system/nginx.service`、`daemon-reload`、`enable` 开机自启），启动阶段优先
> 通过 `systemctl start/restart nginx` 托管启动（`restart` 会先经 `ExecStop=nginx -s stop`
> 停掉 pid 文件指向的旧实例——包括此前手工启动的——再拉起托管实例，避免双实例争抢 80 端口）。
> 若当前存在“手工实例运行中 + systemd 服务循环重启”的状态，执行
> `sudo systemctl restart nginx` 即可一步完成切换；之后可用
> `systemctl status nginx` / `systemctl restart nginx` 管理。

## 三、功能更新：接口文档（2026-11-13）

- **原实现（已删除）**：`gm.gethelp()` 用 `sysinfo.funchtml`（htmlid=100）模板拼接
  `pg_proc/pg_description` 注释后直接返回**整页 HTML**。存在的问题：① 无鉴权，任何请求可调；
  ② 当前库 `funchtml` 为空表，实际返回 NULL；③ HTML 由数据库拼接，前端无法复用与检索。
- **新实现**：
  - 权限节点 **9113「接口文档」**，挂在权限树根「系统 100」之下（`idpath=100.9113`）。
    admin 因持有根节点 100 的授权自动可用（`gm.check_login` 按 idpath 前缀匹配），其他账号按需授权。
  - 新增 **`sysinfo.help_query(p_token)`**（原 gethelp 改名）：校验登录态与权限 9113 后，返回当前库全部业务函数的
    元数据 JSON（`schemaname/funcname/arguments/returntype/description`，随函数增加自动增长）。
    仅含业务 schema：已排除 `crypto`、`gm`（安全/内部工具函数）及 `information_schema`、`pg_catalog`。
  - 新增 **`sysinfo.help_edit(p_token,p_schemaname,p_funcname,p_description,p_arguments)`**（权限 224，挂于 9113 之下）：
    修改指定函数的注释（写 `pg_description`，owner=postgres 以获得系统目录写权限）。
  - 前端「全系统管理」分组新增**接口文档**页面（`#/gethelp`）：表格展示 + 模糊搜索
    （模式/函数名/参数/返回类型/说明全列匹配）+ 分页 + 刷新 + **在线修改函数注释**（权限 224）。
- **涉及文件**：`extension/ngxdb_postgresql.sql`（删 `gm.gethelp`、增 `sysinfo.help_query`/`sysinfo.help_edit` 与
  权限节点 9113/224）、`html/assets/modules.js`（菜单项）、`html/assets/app.js`（renderGetHelp/openHelpEdit）、
  `html/index.html`（资源版本 v=10）。
- **部署注意**：nginx postgres 模块在 **worker 启动时缓存函数清单**，新增接口必须让 nginx
  重新加载后才可访问：
  ```bash
  sudo systemctl restart nginx
  # 若 pid 文件已被清空导致 ExecStop 停不掉手工实例（restart 失败），改用：
  # sudo systemctl stop nginx; sudo pkill -x nginx; sudo rm -f /opt/nginx/logs/nginx.pid; sudo systemctl start nginx
  ```

## 四、安全整改与架构增强（2026-11-13）

### 4.1 等保整改落地
- **连接账号**：nginx 统一使用最小权限账号 `conn`（无业务表读写，仅函数 EXECUTE + 日志 INSERT），旧账号 `ngxapp` 已删除；生产口令随机化并纳入 `secure_deploy` 轮换。
- **请求日志**：修复 `public.serverlog` 无 INSERT 权限导致模块日志静默失败的问题，每次调用的 IP/函数/参数/结果/操作者均落库。
- **暴露面收敛**：接口文档与 nginx 加载清单均排除 `crypto`/`gm` 等内部 schema。

### 4.2 内部函数归拢 gm schema
- 迁移 4 个内部函数：`aes_encrypt`、`aes_decrypt`（token 加解密）、`hash_pass_trigger`（口令哈希触发器）、`_ensure_roles`（安装辅助）→ `gm`。
- 调用方（`sysinfo.login`、`sysinfo.loginaccount`、`gm.check_login`）函数体同步更新；触发器经 oid 自动跟随。
- 迁移后业务函数 **69 个**（nginx 加载清单 = 接口文档），内部函数 12 个全部在 `gm`，不对接口文档与路由暴露。
  （同日新增 `sysinfo.loginlog_query` 后业务函数 **70 个**。）

### 4.3 nginx 模块增强（连接池 + 热管理）
- **连接池**：每个 `postgresconn` location 独立函数表与连接池（相同连接串共享入口），池随负载伸缩（高峰新建、低谷回收），归还前 `ROLLBACK` 失败事务并重置 `app.operatorid`。
- **`/_poolctl` 热管理**：查询各入口池状态与使用统计；`resize`/`on`/`off` 热调池大小与开关（写共享内存页，全 worker 立即生效）；`action=reload` 支持按入口重载函数清单。
- **`/_reloadfunctions` 热重载**：新增/修改数据库函数免重启生效。

### 4.4 SQL 事实源修复（全新安装重放验证 0 错误）
- 补建 `public.serverlog` 表与序列（原为运维手工创建）；
- 新增 `operpermission_permissionid_seq` 的 `setval` 同步（初始数据使用固定 id，避免重放后 nextval 主键冲突）；
- `gm.http_get/http_post` 改为 `verify=False`（内网自签证书）；
- `gm.gettoken/gettokene` 补 `v_cur record` 声明（原 for-in 循环缺变量声明，PL/pgSQL 语法错误）；
- `gm.gettoken/gettokene` 移除对 `pkg_service.job_finish` 的调用（dump 未含该扩展，调度改由外部 cron/pg_cron 驱动）。

### 4.5 日志归拢 public + 按月分区就绪（2026-11-13）
- **三张日志表统一到 public**：`public.serverlog`（nginx 请求日志 + 操作审计，审计行 `pckname='audit'` 区分）、
  `public.loginlog`（登录日志，不含口令）；`sysinfo` 侧旧表结构不再创建。
- **数据迁移幂等**：旧表数据按 `logid+100000` 平移迁入 public，`NOT EXISTS` 防重；迁移 SQL 已在在用库执行
  （serverlog 865+535=1400 行、loginlog 12 行），全新安装无旧表时自动跳过。
- **查询不受影响**：`sysinfo.serverlog_query`（权限 107）改查 `public.serverlog`，前端"系统日志查询"页无需改动。
- **写入方切换**：`gm.audit_trigger`、`sysinfo.login`、`sysinfo.loginaccount` 均改写 public 表；
  `conn` 账号补 public.loginlog INSERT/序列授权，`gm` 补 loginlog/serverlog 读写授权。
- **分区就绪**：日志主键含时间列后即可按月 RANGE 分区（docs/PERFORMANCE.md 2.5），等保"日志留存≥6 个月"
  按"当月主表+历史分区+到期归档"落地。
- `login` 顺带修复：未知子系统/密码错误路径此前不记 loginlog，现统一记录（等保"登录失败处理"审计闭环）。

### 4.6 权限语义明确化、旧表清理与父子表模板修复（2026-11-13）
- **permissiontype 语义统一**：`operpermission.permissiontype` 1=员工直接权限（`sysactionid` 存 `sysaction.sysactionid`）、2=角色权限（`sysactionid` 存 `roleinfo.roleinfoid`）。6 个相关函数（`operinfo_add/edit/merge`、`operinfopermission_add/edit/merge`）的 COMMENT 已按此明确标注。
- **子系统过滤对齐**：`operinfo_add/edit/merge` 的权限 JSON 插入原先未限定子系统，现与 `operinfopermission_*` 对齐（仅当前登录子系统内合法授权），防止跨子系统权限注入。
- **无用表清理**：`sysinfo.serverlog`、`sysinfo.loginlog` 旧表及序列在确认 public 侧数据完整后 DROP；事实源追加幂等 `DROP ... IF EXISTS`，全新安装与存量库重放均一致。
- **gen_module.py 父子表模板修复**：`merge` 原实现（CTE upsert + `currval` 兜底）在"传入不存在 id"时返回的 id 是调用方的无效值；改为 `update → if found → else insert ... returning pk into v_id`，实测 merge(999999) 正确返回新主键。父子表示例（task/subtask）全 CRUD（add/query 联查 upname/edit/merge×2/del/undel/upid=0 顶级）在测试库实测通过。
- **树表循环定义防护补全**：`sysorg_edit/merge` 实测存在"上级改为自身/自身下级"绕过（返回 0 形成环）——修复：recalc 起点兼容 `upid NULL`（`coalesce(upid,0)=0`）、edit/merge 增加自指快查、`sysorg_add` 补 idpath 重算、存量 `sysorg.upid NULL→0` 数据修复；生成器 `tree_recalc`/`_loop_check` 同步。实测：成环/自指→100017，正常编辑→0。
- `sysinfo.loginlog_query` 新增后业务函数 70 个，nginx 热重载生效；前端新增"登录日志查询"页。

**发布决策（v1.0）**：管理接口（`/_reloadfunctions`、`/_poolctl`）保持**仅允许本机（127.0.0.1）调用**，默认不启用 token——方便下载即试；token 鉴权机制已内置（`postgresreloadtoken`），待实践检验后按社区反馈决定是否默认开启。初始账号 `admin/123456` 同理保留为试用默认值，生产上线请修改。

**回归验证**：新建测试库重放 SQL（0 错误）后执行 `test_ngxdb_db.py` 全量用例 **148 通过 / 0 失败 / 0 已知问题**（新增 I4 loginlog_query 用例；gettoken 修复后直接 PASS；regaccount 短信路径容错后 PASS）。

---

## 五、性能手册重写与实测复测（2026-11-14）

`docs/PERFORMANCE.md` 此前混用了"安全验证"与"扩展方案"，且性能数字是早前 116 函数、
无连接池时代的旧值（`help_query 7.8ms`、`50 并发 ≈125 req/s`），与当前实现不符。本次重写为
"实测 → 瓶颈 → 路线"三段式，全部数字在**现行部署上重新实测**。

**实测环境**：4 vCPU / 7GB 虚拟机；nginx 1.30.6（`worker_processes 1`、`worker_connections 1024`）
+ PostgreSQL 18.6（`max_connections=100`、`shared_buffers=128MB`）同机；HTTPS 回环；
`conn` 最小权限账号；`postgrespoolsize 16`；19 张业务表 / 13 MB；业务函数 70 个。

**复测数据（每个请求均含完整鉴权与审计写入）**：

| 接口 | avg | p95 |
|---|---|---|
| `operinfopermission_query` | 2.41 ms | 2.71 ms |
| `sysorg_query`（递归 CTE） | 2.38 ms | 2.57 ms |
| `operinfo_query` | 2.91 ms | 2.98 ms |
| `serverlog_query` | 3.07 ms | 3.87 ms |
| `loginlog_query` | 3.16 ms | 4.02 ms |
| `actions_query` | 3.48 ms | 4.68 ms |
| `help_query`（70 函数全元数据） | 4.23 ms | 5.44 ms |
| `errorcode_query` | 4.44 ms | 13.38 ms |
| `login`（bcrypt + 两层 AES + 写库） | 21.16 ms | 21.95 ms |

并发（限流放行窗口内）：10 并发 100 请求 → **482 req/s**；20 并发 100 请求 → **573 req/s**；
40 req/s 持续压 5 秒 197 次 → **0 失败**，avg 6.39 ms / p95 8.61 ms。

连接池验证：`/_poolctl` 返回 `get=1477 hit=1476 new=1 fail=0 drop=0`（命中率 99.93%），
`pg_stat_activity` 中 `conn` 连接恒为 1 条。

**关键发现（已写入文档）**：单机并发天花板由 nginx `limit_req rate=50r/s burst=100`
（等保限流）决定，**不是程序处理能力**；无节制高并发会得到 `HTTP 503`，`error.log` 记录
`limiting requests ... by zone "apilimit"`，属预期行为。压测与容量规划需先明确限流策略。

**文档改进点**：
- 新增可复现的压测脚本（Python，含 TLS、登录取 token、分位数统计）；
- 明确区分「已内置能力」/「改配置即可」/「需额外组件」，避免读者做重复开发；
- 补齐**本项目特有的**优化项：`worker_processes` 单 worker 是并发延迟主因、
  `public.loginlog(accounts, logintime)` 索引是登录热路径必需、日志表分区 DDL 与归档做法、
  深分页 keyset 改造、`pg_stat_statements` 当前未安装；
- 标注 PgBouncer 的 **`app.operatorid` 会话级变量 → transaction 模式会审计串号** 风险与两种解法；
- 高可用章节明确"实例无状态 ⇔ PG 是唯一状态源"，故 PG 主备优先级高于加 nginx 机器；
- 与 Tomcat 对比保留但标明"非本机同环境实测，仅供选型参考"；
- 文末新增上线前检查清单（每项附自查命令）。

同步更新：README 亮点行与「实测性能」小节改用新数据并加"限流天花板"提示。

---

## 六、员工/角色权限：子表 JSON 一次性保存 + 系统操作员保护（2026-11-14）

**需求**：员工权限与角色权限要支持“修改权限”（权限**子表**的增/改/删）；父子表模板同样要具备
子表 CRUD；系统操作员（10000/100000）不允许删除。前端交互参照 `/opt/nginx/html/bootstrap` 下
的 `operinfopermission.html` / `roleinfo.html`（主表 + 明细子表 + 树选择权限点）。

**实现方式（不新增子表行级函数）**：复用既有的“整表 JSON 提交”契约——

- **保存**：前端明细表格在本页内存里增/改/删权限行，然后把**整个子表数组**序列化为 JSON，
  连同父表主键一次性提交给父表编辑函数，由其在同一事务里按子表主键增删改（删除→更新→插入），原子更新父子表且子表主键保持稳定：
  - 员工权限 → `sysinfo.operinfopermission_edit(operatorid, operpermissionjson)`（权限点 139）
  - 角色权限 → `sysinfo.roleinfo_edit(roleinfoid, roleinfoname, description, rolepermissionjson)`（权限点 132）
- **查询**：`operinfopermission_query` / `roleinfo_query` 本就把每个父行下的全部子表权限行
  聚合为 `operpermissionjson` / `rolepermissionjson` 一次性返回（角色侧还联查 `sysactionname`）。
- **修复一个既有缺陷**：`operinfopermission_query` 分页分支引用了不存在的别名 `toper`，
  导致该接口直接报错（`missing FROM-clause entry for table "toper"`）。已去掉多余列，
  用 `CREATE OR REPLACE` 应用到在用库，SQL 事实源同步修正。
- **曾短暂新增过 6 个行级子表函数（operpermission_* / rolepermission_*），按评审意见回退**：
  已从 SQL 事实源与在用库中删除，函数计数回到 **业务 70**（sysinfo 69 + operator 1）。

**系统内置操作员保护**：`operinfo_del`、`operinfoorg_del`、`operinfopermission_del` 对
`operatorid in (10000,100000)` 直接返回 **100036「系统内置操作员不能删除！」**
（新增错误码 100036）。这三个函数同步改为 `CREATE OR REPLACE`，保证事实源可重放。

**前端（html/assets/app.js + modules.js）**：

- `openDetail` 从只读改为**可编辑明细**：
  - `operpermission` / `rolepermission`：内置 `SUBTABLE` 配置（子表 JSON 字段、父表编辑接口、
    可编辑字段、权限点）。新增/修改/删除都只改内存数组，随后 `saveSubtable()` 把整表 JSON
    一次性提交；`permissiontype=1` 用树选择器选功能权限，`=2` 用列表选角色（`pickPermission`）；
    按操作权限（138/139/141、132/133/135）控制按钮显隐。
  - **父子表模板**：`gen_module.py` 以父表为主模块生成，`modules.js` 里写入
    `SUBTABLECFG.push({...})`；父表“明细”读取父行 `<child>json`，增/改/删后把子表
    整表 JSON 一次性提交给父表 `_edit`（与权限子表同一套机制，不调用子表独立接口）。
- 静态资源版本号升至 `?v=13`。

**生成器（tools/gen_module.py）**：

- `parent_child` 模板改为**以父表为主模块**：父表 `_query` 每行返回 `<child>json`、
  `_add/_edit/_merge` 接收 `p_<child>json json`（同一事务按子表主键增删改，一次性更新父子表）、
  `_del` 带“有子记录禁删”防护；**不再生成子表独立函数**，并追加 `SUBTABLECFG.push(...)`
  供前端明细使用；`--print-sql` 改为纯预览（不再误写 SQL 事实源）；`patch_readme` 的
  模块表追加分隔符改为顿号。示例：`tools/examples/example_parent_child.json`。

**验证**（在用库 + 热重载后实测）：

- 员工权限：一次性提交 2 行（新增权限点）→ 0、回读 2 行且新权限生效；再一次性提交 1 行
  （模拟删除）→ 回读确认已移除；还原原始 JSON → 0。
- 角色权限：新建临时角色 → 一次性提交 1 行 → 0、回读 1 行且含 `sysactionname` 联查；
  清理（清空权限 + 删除角色）→ 0。
- 保护：`operinfo_del`/`operinfopermission_del`/`operinfoorg_del` 传 100000 → **100036**。
- `/_reloadfunctions` 返回 70 个函数；`test_ngxdb_web.py` 回归 34 通过 / 0 失败。
- 测试数据已清理（临时角色与权限变更均已还原）。

**文档**：README、TEST_PLAN（函数分布 82、覆盖矩阵、验证点）、DEVELOPMENT、SECURITY、
skill（SKILL.md/reference.md）同步“子表 JSON 一次性保存”与 10000 保护说明。

## 七、子表保存改为按主键增删改（2026-11-14）

**调整**：员工权限/角色权限/父子表模板的子表保存不再“先删后插”（整表替换会重新生成子表主键，
导致外部按子表主键关联的信息失效），改为**按子表主键做增删改**：

- **删除**：库中该父行下、主键不在提交集合中的行（delete 必须在 insert 之前，
  避免误删本次新插入、主键尚不在提交集合中的行）；
- **更新**：JSON 行携带主键且库中存在 → 按主键 UPDATE（主键保持不变）；
- **插入**：JSON 行主键为空或库中不存在 → INSERT，由序列生成新主键。

涉及函数（`sysinfo`）：`operinfo_edit`、`operinfo_merge`、`operinfopermission_edit`、
`operinfopermission_merge`、`roleinfo_edit`、`roleinfo_merge`；`gen_module.py` 的
`parent_child` 模板（`_child_sync`）同步调整。前端 `app.js` 修改子表行时携带主键提交。

**验证**（HTTP 层，临时员工/角色，测后清理）：新增行后原主键不变、新行有新主键；
修改行主键集合不变且字段更新；删除行按主键移除；角色侧同样通过。`_reloadfunctions` 70 个函数。


---
## 管理台 admin：父子表/树表支持 gm Python（plpython3u）生成（2026-11-14）

**背景**：admin 管理台的“gm Python 模式”此前仅支持单表；现扩展到 **父子表（parent_child）** 与
**树表（tree）**，与 plpgsql 模板（`tools/gen_module.py`）函数签名、错误码、前端 modules.js 配置完全一致。

**新增/改动**：
- `admin/gen_pyfuncs.py`：新增 plpython3u 版父子表（`p_<子表>json` 整表同步、`_query` 返回
  `<子表>json`、有子禁删 100016）与树表（`p_upid`/自定义主键取号、上级校验 100014、循环定义
  100017、`idpath/idlevel/idcount` 重算、有下级禁删 100016、`_query` 联查 `upname` 按 `idpath` 排序）。
- `admin/app.py`：`build_def` 接受 `kind` 并校验树表结构列/子表；`create_table` 支持树表结构列与
  父子表一次建两表；`drop_table` 可连带删子表；`patch_modules_js` 兼容 `SUBTABLECFG.push` 块。
- 前端 `static/`：生成页与新建表弹窗增加“表类型”选择与子表配置（子表选择/主键/外键/字段勾选）。

**顺带修复（两种语言的父子表模板共有，均经实测暴露）**：
1. `json_to_recordset(...) as x(...) x` 子查询**重复别名**导致 SQL 语法错误——去掉多余别名
   （`gen_module.py` 与 `admin/gen_pyfuncs.py` 同步修复）。
2. 子表 `numeric(p,s)` 字段在 `json_to_recordset` 列定义中被归一为 `text`，回写时报
   `DatatypeMismatch`——新增 `_coltype()`，白名单内建表类型原样使用（保留精度）。
3. plpython3u 版 `_query` 的过滤条件改为 `($i is null or ...)`，NULL 参数不过滤，对齐 plpgsql 语义。
4. plpython3u 版 `_del/_undel` 补齐主键存在性检查（100018），对齐 plpgsql。

**测试**（经 nginx `https://127.0.0.1/func/...` 实测，测后全部清理还原）：
- 树表：add 根/子/孙 → `idpath=1 / 1.2 / 1.2.3`、`idlevel/idcount` 正确、`upname` 联查正常；
  循环定义 100017、上级不存在 100014、有下级禁删 100016、merge 增改、删叶子、恢复，共 11 项通过。
- 父子表（plpython3u 与 plpgsql 各一轮）：add 携带子 JSON、query 返回子 JSON、edit 按**子表主键**
  改/删/增（主键保持、数量/金额更新正确）、有子禁删 100016、清空子后删父、恢复，均通过。
- 清理后核对：模块函数数回到 70、测试权限节点 0、测试表 0、modules.js 无残留块。


---
## 管理台 admin：模式（schema）可选/可新建，新模式函数发布即无权限问题（2026-11-14）

**背景**：此前新建表时模式只能手填（默认 `sysinfo`），且新模式下发布的函数存在
“nginx 连接账号（conn）能否 EXECUTE”的隐患。

**新增/改动**：
- `admin/app.py`：新增 `POST /api/schema/create`——创建模式（OWNER gm、可附中文说明注释），
  并执行与 `sysinfo`/`gm` 一致的最小授权：`GRANT USAGE ON SCHEMA ... TO conn`、
  `GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA ... TO conn`、
  `ALTER DEFAULT PRIVILEGES IN SCHEMA ... GRANT EXECUTE ON FUNCTIONS TO conn`（默认权限兜底，
  即使将来全局收回 PUBLIC EXECUTE 也不受影响）；`create_table` 校验模式存在，否则提示先建模式。
- 前端 `static/`：新建表弹窗的“schema”输入框改为**模式下拉**（数据源 `/api/schemas`），
  旁边提供“＋ 新建模式”按钮与新建模式弹窗（模式名/中文说明）；界面文案统一使用中文“模式”。

**验证**（实测后已清理还原）：
1. 新建模式 `sales`（销售业务）→ DDL 含上述三条授权；
2. 在 `sales` 下建表 `t_customer` → 生成 plpython3u 模块 → 发布；
3. 经 nginx `POST /func/sales/t_customer_add`、`/func/sales/t_customer_query` 调用全部
   `errorcode=0`（add 返回 id=1，query 返回 1 行）——新模式发布的函数**无权限问题**；
4. 权限核对：`has_schema_privilege('conn','sales','USAGE')=t`、
   `has_function_privilege('conn','sales.t_customer_query(...)','EXECUTE')=t`；
5. 清理后：函数数回到 70、`sales` 模式已删、测试权限节点 0、modules.js 无残留（项目与部署目录一致）。


---
## 管理台 admin：字段类型扩充 + 长度可自定义 + 同名冲突提示优化（2026-11-14）

**背景**：新建表的字段类型仅 9 种固定选项（长度不可调）；且“字段名与结构列冲突”报错
未说明原因（实际多为业务字段与默认主键“表名+id”同名，如 `dict` 表默认主键 `dictid`）。

**新增/改动**：
- 类型白名单统一收敛到 `tools/gen_module.py` 的 `COL_TYPE_RE`（admin 建表/改列与模板生成器
  共用一份），扩充至 PostgreSQL 常用类型：`char(n)`、`real`、`double precision`、`money`、
  `time`、`interval`、`uuid`、`json/jsonb`、`bytea`、`inet/macaddr/cidr`、`xml`、`serial/bigserial`。
- 前端新建表：类型改为 25 种带中文说明的下拉；`varchar/char/numeric` 旁增加**长度/精度输入框**
  （按类型显隐、带默认值），提交时拼接为 `varchar(50)`/`numeric(10,3)` 等。
- `pg_type()`：`char(n)/varchar(n)/numeric(p,s)` 保留长度定义（此前 `numeric(p,s)` 被归一为
  `text`、`char(n)` 被截为 `character(1)`，导致函数参数类型错误/数据截断）；`timestamptz`
  正确映射 `timestamp with time zone`；新增 real/double precision/uuid/json(b)/bytea/inet/
  macaddr/cidr/money/xml/time/interval/serial/bigserial 映射。
- `admin/app.py` 发布流程：应用 SQL 前**先删除同 schema 下同名六函数的所有旧签名**
  （字段类型修改后 `CREATE OR REPLACE` 因签名不同会残留重载函数，导致调用命中旧版）。
- 冲突提示细分：字段与主键同名 → 提示“主键默认为‘表名+id’，可在‘主键名’中另行指定”；
  与系统结构列（isused/systemid/upid/idpath/idlevel/idcount）同名 → 提示修改字段名。

**验证**（实测后已清理还原）：
- 新建 `t_type_demo`（varchar(50)/char(4)/numeric(10,3)/uuid/jsonb/timestamptz/money/inet）
  → plpython3u 生成发布 → 经 nginx add/query：各类型写入与读回正确（char 不再截断、
  numeric 精度正确、uuid/jsonb/timestamptz/money/inet 均正常）；
- 同名冲突：字段与主键同名 → 返回细分后的中文提示；
- 发布后旧签名重载函数已被清理（同名函数仅 1 个）；清理测试数据后函数数回到 70、
  modules.js 无残留（项目与部署目录一致）。


---
## 管理台 admin：模块生成改为权限树挂载，菜单ID与权限ID合一（2026-11-14）

**背景**：生成页原用“分组”下拉（9001/9002/9003）+ 手填“菜单ID”两个字段决定权限挂载；
用户要求以**权限树**直观选择挂载位置，且菜单ID与权限ID本为同一 ID，无需重复填写。

**新增/改动**：
- `admin/app.py`：新增 `GET /api/perm/tree`（返回 sysinfo.sysaction 全量节点，按 idpath 排序）；
  `build_def` 改为从权限树所选节点挂载——校验节点存在/启用，查询其 `idpath` 存入
  `parent_idpath`；`menu_action`/`actions` 未提供时自动分配空闲 ID（`/api/ids` 推荐）。
- `tools/gen_module.py` 的 `gen_sysaction()`：支持任意父节点挂载——菜单/操作节点的
  `idpath/idlevel` 按父节点 `idpath` 动态计算（默认 `100.<分组>` 行为不变，完全兼容）。
- 前端：生成页删除“分组”下拉与“菜单ID”输入框，新增**权限树**（按层级缩进、点选高亮、
  默认选中 9003 公共数据），下方提示“挂载于 …；本功能权限/菜单ID自动分配为 …”；
  每次生成前自动刷新空闲 ID 推荐。

**验证**（实测后已清理还原）：
- 挂载到深层节点 9100（系统权限，idpath=100.9001.9100）下生成并发布：菜单节点 9113
  `idpath=100.9001.9100.9113、idlevel=4、upid=9100`，6 个操作节点 `idlevel=5、upid=9113`，
  全部正确写入 sysaction；
- 默认挂载（9003）行为与原逻辑一致（idpath=100.9003.<菜单ID>、idlevel=3）；
- 清理后：函数数 70、测试权限节点 0、测试表 0、modules.js 无残留（项目与部署目录一致）。


---
## 管理台 admin：权限树节点可直接进入“修改权限”，同事务更新 sysaction 与 actions（2026-11-14）

**背景**：权限由两表组成——`sysinfo.sysaction` 负责把权限**组织成树**（idpath/idlevel/upid），
`sysinfo.actions` 是**具体权限**（actionname/description/actionurl/params/code，经
`sysaction.actionid = actions.actionid` 关联）。此前管理台只能查看权限树，不能修改节点。

**新增/改动**：
- `admin/app.py`：
  - `GET /api/perm/node?sysactionid=`：返回树节点详情 + 关联的 actions 具体权限（无则 null）；
  - `POST /api/perm/update`：**同一事务**内更新两表——sysaction 侧可改 `sysactionname/isused`，
    actions 侧可改 `actionname/description/actionurl/params/code/isused`；节点无 actionid
    （纯目录节点）或 actions 无对应行时仅更新树节点并明确提示。
- 前端：权限树每个节点增加“修改”按钮 → 打开“修改权限”弹窗（只读展示 idpath/层级/上级；
  树节点表单 + 具体权限表单分区；保存后刷新权限树并提示两表更新结果）。

**验证**（以节点 188“新增市”实测，测后已还原）：
- `perm/node` 回填：node（idpath=100.9003.9112.188、actionid=188）+ action（actionurl=sysinfo/city_add）；
- `perm/update` 同事务改 `sysactionname=新增市(测)`、`actionname=新增市X`、`description=测试说明`
  → 查库两表均更新；仅改 isused 时 actions 不受影响（changed=1）；
- 修复两处笔误（`aset`→`asets`）——其中一处位于 return，曾导致“更新已提交但响应 500”；
- 测试数据已还原（节点名/权限名恢复原值、description 置回 NULL）。


---
## 管理台 admin：权限树增强——模糊搜索定位、增删改按钮、同级上移/下移（2026-11-14）

**新增/改动**：
- 权限树上方新增**模糊搜索框**：按权限名称/ID/idpath 过滤，自动保留命中节点的祖先链并
  `scrollIntoView` 定位到第一个命中节点；
- 权限树下方新增按钮组：**新增子节点 / 删除 / 修改 / ↑上移 / ↓下移**（均作用于点选的节点）；
  “修改”即既有弹窗（sysaction + actions 两表同事务更新）；
- 新增 `POST /api/perm/add`：在选中节点下创建子节点（ID 取全表 max+1），可选同时创建
  `actions` 具体权限，并同步父节点 `idcount`；
- 新增 `POST /api/perm/delete`：**逻辑删除**选中节点及整个子树（isused=0，actions 同步停用）；
  系统核心节点（100/9001/9002/9003）禁止删除；
- 新增 `POST /api/perm/move`：同级上移/下移。因树的顺序由 `idpath` 字典序决定、且 idpath 跟随
  sysactionid 走，**单纯交换 sysactionid 无法改变顺序**——最终实现为交换两棵子树的
  **idpath 前缀**（经临时段 `@tmp1@` 三步换位），节点 id/upid/actionid 及 actions 关联全部不变；
  兄弟节点按“upid + idlevel + 父 idpath 前缀”过滤（兼容 224 这类历史孤儿节点）。

**验证**（以测试模块节点 9113/225/226 实测，测后已清理还原）：
- 新增子节点（带具体权限）→ sysaction 与 actions 两表均写入；
- 225 下移 → 225/226 的 idpath 互换（225=.226、226=.225），树上顺序真实改变；再上移还原；
- 边界：第一位上移、最后一位下移均被拒绝并提示；核心节点删除被拒绝；
- 逻辑删除：节点及子树 isused=0、actions 同步停用；
- 清理后：函数数 70、测试权限/具体权限节点 0、测试表 0、modules.js 无残留（两处一致）。


---
## 管理台 admin：权限树渲染与交互优化（2026-11-14）

**改动**：
- 权限树改为**树状渲染**（└─/├─/│ 连接线按层级绘制，替代纯缩进）；
- 节点行内的“修改”小按钮**移除**，修改入口统一为树下方的“修改”按钮（作用于点选节点）；
- **上移/下移语义调整**：在**搜索出多个结果**时，↑/↓ 作用于搜索结果序列——将当前定位的
  命中节点与搜索结果中的上一个/下一个命中节点交换树上位置（后端 `/api/perm/move` 改为
  `sysactionid + target` 两个同级节点交换 idpath 前缀；不同级/父子关系会被拒绝并提示）；
  无搜索时仍按真实同级相邻节点移动。

**验证**（临时节点 9200/9201 实测后已删除）：
- 下移：9200/9201 的 idpath 成功互换（100.9001.9100.9201 / …9200），树上顺序真实改变；
- 非同级（父节点 9100 与其子节点 9200）移动被拒绝；
- 上移还原成功；前端资源 v7 已发布，后端正常。


---
## 管理台 admin：权限树配色修正、按钮布局调整、action 同步修改改为可选开关（2026-11-14）

**反馈与修正**：
1. 权限树此前为白底浅色，与页面深色主题不符且看不清——树容器/节点/连接线/折叠符/搜索框
   全部改为主题变量配色（--code/--line/--tx/--dim/--pri）；
2. ↑上移/↓下移按钮**移到搜索输入框后**（对搜索出的多个结果排序）；树下按钮组保留
   新增子节点/删除/修改；
3. 修改权限弹窗新增 **“同步修改 action”开关**（默认关闭）：
   - 勾选：显示 actions 全部字段（actionname/actionurl/description/code/params/isused），
     保存时**同一事务同时更新 sysaction 与 actions**（actionid 共用不变）；
   - 不勾选：仅更新 sysaction 树节点信息；
   - 节点无 actionid 或 actions 无对应行时，勾选会提示“无具体权限记录”，仅可改树节点信息；
   - 新增权限节点的“同时创建具体权限”选项保持不变。

**验证**（节点 188 实测后已还原）：
- 不勾选同步：仅 sysaction 更新（actions_updated=false），actions 表数据不变；
- 勾选同步：sysaction 与 actions 同事务更新（actionname/description 生效）；
- 页面已发布（admin.js?v=9），新布局与开关元素均已上线。


---
## 管理台 admin：建模页增加“系统”字段、系统管理、操作权限表格化（2026-11-14）

**新增/改动**：
- 建模页新增**系统（权限归属）**下拉（数据源 `sysinfo.systeminfo`，默认 100 维护系统），
  生成的权限节点 `systemid` 跟随所选系统；
- 新增**系统管理**弹窗（“系统管理”按钮）：列出/新增/修改/删除 `systeminfo`——
  新增需填系统名称与 prikey（加密密钥，算法默认 1，ID 默认 max+1）；删除前校验该系统下
  是否仍有权限节点（有则拒绝）；列表不显示 prikey 明文；
- **修改权限弹窗重构为表格**：按 `/api/perm/group` 返回功能整组权限——菜单节点表单 +
  操作权限表格（每行：操作类型（由 actionurl 后缀映射 新增/修改/合并/删除/恢复/查询）、
  actionid、权限名称（默认“新增xxx”等生成名，可改）、接口地址、说明、启用）；
  保存走 `POST /api/perm/update_group`，**同一事务**更新 sysaction（菜单+各操作节点名称/启用）
  与 actions（actionname/actionurl/description）；actions 无对应行时自动补建（actionid 共用）；
  校验操作节点必须属于该菜单（防止越权改其它节点）。

**验证**（测试系统 101 与测试模块 t_sys_t 实测，测后已清理还原）：
- 系统创建/修改/列表正常；删除有权限节点的系统被拒绝；
- `systemid=101` 生成发布 → sysaction 全部节点 systemid=101 正确；
- `perm/group` 返回菜单+6 操作节点；`update_group` 修改操作节点名称并补建 actions 行
  （actionurl/actionname/description）生效；
- 清理后：函数数 70、systeminfo 仅剩 100、测试权限节点 0、modules.js 无残留（两处一致）。
- 系统管理弹窗**新增系统时 prikey 默认自动生成 16 位随机密钥**（`crypto.getRandomValues`，
  字符集去除易混淆字符；与 `prikey varchar(16)` 列长匹配），可手工修改；README「五、安全提示」
  与「十四、安全与性能验证」已补充 `prikey`/`algorithm` 的用途说明（子系统 token 内层
  AES 密钥；算法 1=AES-128、2=AES-256；修改会使该系统在线 token 全部失效）。



---
## 管理台 admin：新建表不再强制 isused/systemid，改为可选快捷列；生成模板适配（2026-11-15）

**背景**：有的表数据可物理删除、不需要 `isused`；有的表多系统共享、不需要 `systemid`。
此前新建表会强制追加这两列，且生成模板强依赖它们。

**改动**：
- `admin/app.py` `create_table`：**不再自动追加** `isused`/`systemid`；新建表弹窗提供快捷按钮
  “＋ 启用列(isused)”、“＋ 系统列(systemid)”，由用户决定是否添加（父子表的子表跟随同一开关）；
  `build_def` 不再强制校验两列，改为按表实际列写入 `has_isused`/`has_sysid` 标记。
- `tools/gen_module.py` 与 `admin/gen_pyfuncs.py`（plpgsql/plpython3u 两套模板同步）按
  `has_isused`/`has_sysid` 条件生成：
  - 无 `isused`：`del` 为**物理 DELETE**；不生成 `undel` 函数（modules.js 中 undel 置 null，
    前端不显示恢复按钮）；query 不带 isused 过滤（保留 `p_isused` 形参以兼容前端传参）；
  - 无 `systemid`：insert/merge 不写 systemid；
  - 默认（标记缺省）行为与原来完全一致，存量模块不受影响。
- 修改表（`POST /api/table/update_fields`）：结构与主键列保护、业务列按提交 diff
  生成 ADD/ALTER TYPE/COMMENT/DROP，事务执行。

**验证**（无两列测试表 t_noflag2 实测，plpython3u 发布 + nginx 调用，测后已清理）：
- 建表 DDL 不含两列；生成的 add/query/del 函数体不引用 isused/systemid；
- add/query 正常；`del` 后行**物理消失**（query total=0）；`undel` 不存在（连接关闭，符合预期）；
- 清理后：函数数 70、测试表 0、测试权限节点 0、modules.js 无残留（两处一致）。


---
## 管理台 admin：修复新增权限节点误挂历史子节点（2026-11-15）

**现象**：权限树新增节点 ERP（自动分配 ID 9113）后，ERP 下多出历史节点“修改接口注释”（224）。

**根因**：224 的 `idpath=100.9113.224`（原属已删除的“接口注释”功能），父节点删除后残留；
新节点复用了 ID 9113，按 `upid=9113` 归属，224 便“挂”到了 ERP 之下。

**修复**：
- 清理残留的历史节点 224（sysaction 与 actions 同步删除）；
- `perm_add` 自动分配 ID 时**跳过任何现存 idpath 中已引用的 ID 段**
  （如 9114 的 idpath 含 9113，则新节点不再分配 9113），防止同类问题复发。

**验证**：新增测试子节点分配到 9115（未复用 9113/9114），确认后已删除；ERP 下仅剩“字典”节点。


---
## 管理台 admin：新建表结构列改为可选；生成模板适配无 isused/systemid 的表（2026-11-15）

**背景**：有的表数据可物理删除、不需要 `isused`；有的表多系统共享、不需要 `systemid`。
此前新建表会强制追加这两列，且生成模板强依赖它们。

**改动**：
- `admin/app.py` `create_table`：不再自动追加 `isused`/`systemid`；新建表弹窗提供快捷按钮
  “＋ 启用列(isused)”、“＋ 系统列(systemid)”，由用户决定是否添加（父子表的子表跟随同一开关）；
  `build_def` 不再强制校验两列，按表实际列写入 `has_isused`/`has_sysid` 标记。
- `tools/gen_module.py` 与 `admin/gen_pyfuncs.py`（plpgsql/plpython3u 模板同步）按标记条件生成：
  - 无 `isused`：`del` 为**物理 DELETE**；不生成 `undel`（modules.js 中 undel 置 null，
    前端不显示恢复按钮）；query 不带 isused 过滤（保留 `p_isused` 形参兼容前端传参）；
  - 无 `systemid`：insert/merge 不写 systemid；
  - 默认（标记缺省）行为与原来完全一致，存量模块不受影响。
- 顺带修复：类型校验归一化（`timestamp without time zone` 等带空格类型此前会被误判不支持，
  导致含时间戳/浮点字段的表点生成报错）。

**验证**（无两列测试表 t_noflag2 实测，plpython3u 发布 + nginx 调用，测后已清理）：
- 建表 DDL 不含两列；生成函数体不引用 isused/systemid；del 为物理 DELETE（删后 query total=0）；
  undel 不存在（调用连接关闭，符合预期）；
- 全库表 × 2 语言生成回归通过（仅无主键表 operaccounts 仍不可建模，属预期）；
- 清理后：函数数 70、测试表 0、测试权限节点 0、modules.js 无残留（两处一致）。

---
## 管理台 admin：生成权限时同步写入 actions 表（2026-11-15）

**背景**：`gen_sysaction` 此前只生成 `sysaction`（权限树）INSERT，操作节点在
`sysinfo.actions`（具体权限表）中没有对应记录，需手工补建。

**改动**：`tools/gen_module.py` `gen_sysaction()` 对每个操作节点（actionid 非空）同步生成
`INSERT INTO sysinfo.actions (isused, actionid, actionname, actionurl)`，
actionurl 为 `schema/table_op`；`WHERE NOT EXISTS` 保证重复发布幂等。

**验证**：t_noflag2 模块发布 → actions 表新增 6 行（actionid=194-199，
actionurl=sysinfo/t_noflag2_add/edit/merge/del/undel/query）；
重复发布因 actionid 重新分配产生了一组新节点（194-199 与 200-205 两组）——
**重复发布同一模块会产生重复权限节点**为既有行为（sysactionid 每次自动分配），
提醒：同一模块请勿反复发布；如需固定 ID 可在生成参数中指定 menu_action/actions。
测后已全部清理（函数数 70、sysaction/actions 无残留、modules.js 两处一致）。

---
## 管理台 admin：权限新增/修改不再创建 actions 记录；actions 按 actionurl 判重（2026-11-15）

**改动**：
- 权限树“新增子节点”去掉“同时创建具体权限”选项——新增仅写 `sysaction`（树节点，actionid 为空）；
- `perm_update_group` 去掉 actions 无对应行时的补建 INSERT——操作权限表格仅**更新已存在**的
  actions 行（actionid 共用不变）；
- `gen_sysaction` 生成的 actions INSERT 增加**按 actionurl 判重**（`WHERE NOT EXISTS ... 
  actionid=... OR actionurl=...`），同一接口权限不会被重复插入。

**验证**：perm/add 仅写 sysaction；模块发布 actions 新增 6 行；重复发布后仍为 6 行（判重生效）。

---
## 管理台 admin：权限删除改为物理删除；ngxdb 左侧菜单按权限树深层渲染（2026-11-15）

**改动**：
- `perm/delete` 由逻辑删除（isused=0）改为**物理删除**：选中节点及整个子树的 sysaction 行
  直接删除，actions 具体权限同步删除（核心节点 100/9001/9002/9003 仍禁止删除）；
- ngxdb 前端左侧菜单（app.js renderNavFromRows）：去掉“操作级（level4）不上菜单”的限制——
  建模模块的菜单节点无论挂在权限树哪一层（如 ERP→字典→模块），都会在左侧导航显示
  （节点名称取 sysaction.sysactionname，点击路由到对应模块页面）。

**验证**：临时子节点 9118 新增→物理删除（库中无残留）；9114↔9118 同级交换（下移/上移还原）
正确；9114 与 9116（不同父）移动被拒绝；前端 v44 已发布。

---
## ngxdb 前端：菜单按 sysaction 树全量渲染（空页面占位）；员工权限/角色新增与修改随子表 JSON 提交（2026-11-15）

**改动**：
- 左侧菜单：按 sysaction 权限树渲染所有有权限的节点（名称取 sysactionname）——有对应模块的
  节点点击进入模块页面（子页面代码不变）；没有模块页面的节点进入**空占位页**（blank-<id>，
  提示“该功能页面暂未提供”）。
- 员工权限（operinfo）/角色（roleinfo）新增与修改：表单内嵌**权限明细编辑区**，保存时子表
  整表 JSON（p_operpermissionjson / p_rolepermissionjson）随主表一起提交到 _add/_edit；
  抽出通用子表行编辑弹窗 openSubRowEditor（详情抽屉与表单内嵌区共用）。
- 修复：SUBTABLE.operpermission.editApi 误指 operinfopermission_edit → 改为 operinfo_edit，
  extra 补齐 operinfo_edit 所需父行字段（operatorname/accounts/tokentype/tokeninterval）；
  operinfo 模块补上 dt='operpermission'（行操作出现“权限”按钮）。
- 后端：sysinfo.operinfo_query 增加 operpermissionjson 聚合列（联 sysaction/operinfo 取
  sysactionname），与 roleinfo_query 的 rolepermissionjson 对齐；已回写
  extension/ngxdb_postgresql.sql 并热重载。

**验证**：operinfo_edit 整表 JSON 提交——原行更新、新行（序列主键）插入、差集删除均正确；
operinfo_query 返回 operpermissionjson；前端 v45 已发布。

---
## ngxdb 前端：菜单渲染规则修正——指向函数的操作级节点不上菜单（2026-11-15）

按需求澄清修正左侧菜单生成规则：按 sysaction 树生成菜单，但**节点若指向具体函数
（actionid 非空）即视为尾部（操作级），不生成菜单**——菜单到功能级为止。
例：ERP→字典，字典页面已实现全部函数（新增/修改/合并/删除/恢复/查询字典等操作节点
actionid 均非空），菜单只显示到“字典”这一级，操作节点不再以空页面形式出现在菜单里。
功能节点：有对应模块页面的进入模块页（子页面代码不变）；暂无模块页面的显示空占位页。

前端 app.js renderNavFromRows 已按此修改并发布（资源版本 v48）。

---
## ngxdb 前端：权限名/出错代码/系统日志 三个页面对应到权限树菜单（2026-11-15）

把三个既有模块的菜单挂载点（menu）改到 sysaction 树上对应的功能节点：

- actions（功能查询，即“权限名”页面）：menu 181 → **9108 权限名**；
- errorcode（出错代码）：menu 180 → **9109 出错代码**；
- serverlog（系统日志查询）：menu 107 → **9110 系统日志**。

原先的 180/181/107 是这三个功能下的“查询…”操作级节点（actionid 非空，不上菜单），
导致三个页面在左侧菜单中无处挂载。修改后“系统管理”组下显示：系统权限、子系统管理、
员工所属系统、权限名、出错代码、系统日志；其下“查询…”等操作节点仍按规则不生成菜单。
资源版本 v49。

---
## 建模生成：为新建表在 errorcode 中登记专属出错代码（去重），修复报错信息串台（2026-11-15）

**问题**：建模生成的函数里“记录不存在/主键已存在”复用通用码 100018，而 errorcode 表中
100018 的消息是“部门信息出错！”（sysorg 专属）——新表报错时前端显示错误信息。

**改动**（tools/gen_module.py + admin/app.py，plpgsql 与 gm Python 两条生成路径均生效）：
- 新增 `alloc_error_code(d, qfunc)`：errorcode 表中已有该表（schema='模式.表'）记录则复用
  （重复发布/生成幂等去重）；否则取 max(errorcode)+1 新增“<表中文名>信息出错！”
  （primekey=主键列，schema=模式.表）；
- 新增 `apply_error_code(sql, code)`：把生成函数体中的 100018 替换为该表专属码
  （100014/100016/100017 为通用语义码，保持不变）；
- 生成 SQL 末尾附加去重 INSERT（`WHERE NOT EXISTS` 双保险）；/api/gen、/api/publish、
  tools CLI（--print-sql/--apply）均走该逻辑；
- 存量修正：erp.sys_dict 六函数中的 100018 已替换为 100038，并登记
  errorcode 100038“字典信息出错！”。

**验证**：t_errtest 生成→INSERT 两次仅落一条（去重）；sys_dict_edit 传不存在 id 返回
`100038 字典信息出错！`（原为 100018 部门信息出错！）。

---
## 修复：系统日志页面打开出错（2026-11-15）

**现象**：点击“系统日志”菜单报错，接口返回
`ERROR: array size exceeds the maximum allowed (1073741823)`。

**根因**：nginx 请求日志把 serverlog_query 的**完整响应体**记入 public.serverlog.res；
“全量查日志→响应被记入日志→日志更大→下次全量更大”滚雪球，已产生 537MB/212MB/87MB
三条日志行，全量 array_agg 超过 1GB 上限。

**修复**：
1. 清理 3 条超大异常日志行（均为 2026-11-14 测试时段产生）；
2. `sysinfo.serverlog_query`：content/params/res/head 四个大字段查询结果截断至 2000 字符
   （两分支同步修改；已回写 extension/ngxdb_postgresql.sql 并热重载）——响应从 ~838MB 降到 ~1.4MB，
   且日志记录的响应随之变小，不再雪崩；
3. 前端 serverlog 页面改为**后端分页**（rows=20&page=N，翻页重发请求），单次响应约 100KB。

页面已恢复正常打开（total=1435，errorcode=0）。

---
## 修复：分页页码省略号重复渲染（2026-11-15）

分页按钮生成逻辑用 `btns.slice(-1)!=='…'` 判断是否已输出省略号，但 slice(-1) 取的是
HTML 字符串最后一个字符（恒为 `>`），判断永不成立——页数较多时中间出现大量重复的
“…”按钮（系统日志 72 页时约 68 个）。改为 `dots` 标志位：连续跳页只输出一个“…”。
通用列表分页（renderPager）与 API 帮助页分页（renderApiRows）两处同步修复；
模拟验证 pages=72 各页码位置输出正确。前端 v51。

---
## 修复：员工权限/角色“权限”明细看不到数据（2026-11-15）

**根因**：nginx 对 postgres 的 json 列返回的是**嵌套 JSON 数组**（不是字符串），而前端
openDetail/openForm 里用 `JSON.parse(row[jsonKey])` 解析——对数组调用 JSON.parse 抛异常
被 catch 吞掉，子表行被置空，页面显示“未分配权限”。

**修复**：新增 `parseSubRows()` 兼容解析（数组直接用 / 字符串才 JSON.parse），
权限明细抽屉与新增/编辑表单内嵌明细区两处统一改用；
gen_module 生成的父子表模块（SUBTABLECFG）同样受益。前端 v52。

---
## 修复：权限明细抽屉上弹出的“新增/修改权限”框保存/取消后全部消失（2026-11-15）

**根因**：Modal 为单实例实现——`Modal.open()` 第一步先 `close()` 旧弹窗，导致在“权限”
明细抽屉上打开行编辑框时父抽屉被关闭；行编辑框关闭后父抽屉也不会回来。

**改动**：
- Modal 改为**栈式多实例**：open 不再关闭旧层，close() 关最上层、close(m) 关指定层，
  新层 z-index 递增；mask 点击/右上角关闭均只关本层；
- delSubRow 删除多余的 Modal.close()（确认框自带关闭，再关会误关父抽屉）；
- refreshDetail 改为**原地刷新**（重新拉取父行后更新 ctx.row/子表行并重渲染），
  不再重开抽屉。

效果：明细抽屉上的新增/修改/删除权限操作完成后，抽屉保持打开且子表即时刷新。
前端 core.js/app.js v53 已发布。

---
## 检查与验证：角色权限链路、建模父子表/树表（2026-11-15）

**1) 角色权限检查（与员工权限同类问题排查）**：
- 前端角色权限与员工权限走同一套机制（权限明细抽屉、parseSubRows 兼容解析、栈式 Modal、
  整表 JSON 保存、原地刷新），此前的全局修复已全部覆盖；
- 后端 roleinfo_edit（权限点 132）实测：子表 JSON 新增（功能权限+继承角色）、按主键修改、
  空数组差集删除均正确；继承自己/成环被 100017 拒绝（合理防护）；
- roleinfo_query 返回 rolepermissionjson 为嵌套数组（含 sysactionname），与前端兼容解析匹配；
- pickPermission 支持 type=2（列角色）与 type=1（权限树）。

**2) 建模页面父子表/树表**：功能已具备（新建表与模块生成的“表类型”均含单表/父子表/树表；
父子表一次请求同时建父表+子表）。本次以管理台 API 端到端实测：树表（自动带
upid/idpath/idlevel/idcount、上级校验 100014、循环定义 100017、有下级禁删 100016、
upname 联查按 idpath 排序）与父子表（子表整表 JSON 同步、<子表>json 查询返回、
SUBTABLECFG 前端挂载、有子记录禁删 100016）生成 SQL 均正确，并已事务试跑（BEGIN/ROLLBACK）
验证可执行；测试表已清理。

---
## 修复：角色新增/修改弹窗内权限明细表格超出页面（2026-11-15）

- 带权限明细编辑区的弹窗强制使用宽版（wide，900px）——角色 fields 仅 4 列时此前落入
  620px 窄弹窗，明细表格横向撑出页面；
- `.modal__body` 增加 `max-height:calc(100vh - 230px);overflow:auto`：弹窗内容（含明细
  表格）在弹窗内部横向/纵向滚动，不再超出视口；所有弹窗通用受益。前端 v54。

---
## 管理台：新建表默认继承数据表页面的模式；树表展示默认结构列（2026-11-15）

1. 新建表弹窗打开时，若数据表页面已选择具体模式（schema-sel），新建表的“模式”下拉
   默认选中该模式（选“全部模式”时保持原默认 sysinfo）；
2. 表类型选“树表”时，弹窗内显示“树表结构列”只读区：upid、idpath、idlevel、idcount
   四列自动创建、无需填写（后端建表本就自动附加，此处让流程可见）；修改树表时
   类型正确显示为“树表”（结构列已在字段列表只读展示）；

admin 前端已发布（admin.js v18）。

---
## 新功能：外键引用（建模生成函数/页面支持外键字段）（2026-11-15）

**元数据表 `public.fk_meta`**（已建表并写入 dump）：主模式/主表/主字段（被引用方）、
主字段是否可多选（master_multi，1=可多选，逗号分隔存储）、外键模式/外键表/外键字段
（引用方）、主表显示字段（display_columns，逗号分隔一个或多个）、主表前端模块 id
（master_module，用于页面选择器）。唯一索引 (fk_schema,fk_table,fk_column)。

**生成模板（tools/gen_module.py，plpgsql 路径）**：
- query：外键列 left join 主表，返回主表显示字段（别名 `外键列_显示字段`）；多选外键用
  标量子查询 string_agg 聚合（不展开行）；
- add/edit/merge：写入前校验外键值在主表存在（多选拆逗号逐值校验），失败返回
  100039「关联数据不存在！」（errorcode 已登记）；
- modules.js：外键字段输出 `tp:'主表模块'`（页面选择器）、非树主表附 `lp:1`（列表选择器）、
  多选附 `multi:1`；列表 cols 在外键列后追加显示列。

**前端（app.js）**：新增 `openListPicker`（非树主表的列表选择弹窗）；表单外键字段点击
“选择”按配置走树选择器/列表选择器，多选字段点选后逗号累积（去重）。

**验证**：以 t_fkdemo（ref_oper 单值→operinfo、ref_multi 多选→operinfo）端到端实测：
生成 SQL 含 join/lateral 替代/100039 校验/显示列；事务内 add 正确值→0、坏外键→100039、
坏多选→100039；query 返回 ref_oper_operatorname/ref_oper_accounts 与多选聚合名，
单行不展开。测试表与示例配置已清理。注：gm Python 模式（plpython3u）暂不支持外键。

---
## 管理台：模块生成固定使用 plpgsql，移除函数语言选项（2026-11-15）

按需求移除“模块生成”页的函数语言下拉（plpgsql/plpython3u），生成统一使用 plpgsql。
后端 lang 参数保留兼容（缺省即 plpgsql）；gen_pyfuncs.py 生成器保留但不再接入 UI。
admin 前端 v19 已发布；README/admin README 同步更新。

---
## 管理台：建表/修改表支持录入外键信息（2026-11-15）

- 新建表弹窗新增“外键”区块：可录入 外键字段 → 主表.主字段（可多选）、主表显示字段、
  主表模块 id；创建时随建表 SQL 事务写入 public.fk_meta（自动补主字段=主表主键、
  校验主表存在/标识符合法/禁止引用本表）；
- 修改表弹窗打开时从 public.fk_meta 读取该表外键配置并显示（可编辑），保存结构时
  一并保存外键配置（/api/fk/save，全量替换+事务）；
- 新增接口：GET /api/fk?schema=&table=、POST /api/fk/save；
- 生成模板按 fk_meta 自动应用外键（校验 100039/联查显示字段/页面选择器），形成闭环。
admin 前端 v20（admin.js）/v5（admin.css）已发布。

---
## 确认：外键仅做元数据记录，不生成数据库级外键约束（2026-11-15）

按要求确认并写入文档：`public.fk_meta` 只记录外键引用关系（元数据），**不在数据库中
创建任何 FOREIGN KEY 约束**（fk_meta 自身与建模生成的表均无外键约束，全库经
pg_constraint 检查为 0）；引用完整性由生成函数在写入时校验（100039），分库场景经
postgres_fdw 校验。admin/README 与 docs/DEVELOPMENT.md 已补充该说明。

---
## 修复：管理台“加外键”按钮点击无反应（2026-11-15）

原因：浏览器缓存的旧 index.html 中没有“加外键”按钮元素，admin.js 里
`$('btn-add-fk').onclick = ...` 对 null 赋值抛 TypeError，导致其后的事件绑定全部中断。
修复：新增 `bindBtn(id, fn)` 安全绑定（元素存在才绑定），新增的加外键/分库外键(FDW)
按钮均改为安全绑定，避免个别元素缺失中断整个脚本；admin 前端 v22 已发布
（若仍见旧页面请 Ctrl+F5 强制刷新）。

---
## 修复：管理台页面未更新导致“加外键”无效（2026-11-15）

排查发现：手工同步时只更新了 admin/static/ 下的文件，而 nginx 的 /ngxdb/admin/ 服务的是
admin/index.html——该文件仍是旧版（无外键区块/分库外键按钮），导致新功能“看不到、点了
没反应”。已将 admin/static/index.html 同步到 admin/index.html（以后建议直接执行
bash admin/deploy.sh 部署，避免遗漏）。

---
## 管理台 v23：页面顶部 JS 错误可见化 + 版本徽标（2026-11-15）

- index.html 顶部新增 JS 错误红条（在 admin.js 之前注册监听，语法/运行错误均会显示
  “JS 错误: … @ 文件:行号”），便于远程定位“按钮没反应”类问题；
- 顶部徽标新增版本号（当前 v23），刷新后可直接确认加载的是否为最新页面/脚本；
- 部署目录 admin/index.html 已同步至 v23（此前手工同步遗漏该文件导致线上页面偏旧）。

---
## 修复：加外键行报 “appendChild is not a function”（2026-11-15）

addFkRow 的 box 参数调用方传的是选择器字符串（'#nt-fk-list'），函数内直接对其调用
appendChild 导致 TypeError（页面顶部错误条已捕获显示）。已修复：box 为字符串时先
`$(box)` 取元素再挂载。admin 前端 v24 已发布。

---
## 修复：加外键仍报 null.appendChild（v25，2026-11-15）

上一修复只处理了 box 为元素的情况；调用方传的是“#nt-fk-list”字符串，而 `$()` 是
getElementById 封装（不接受 “#” 前缀）导致仍取到 null。已统一：字符串选择器先去掉
“#” 再取元素（addFkRow 与 collectFks 两处）。admin 前端 v25 已发布。

---
## 管理台：发布入口统一为 8765；外键编辑增强（2026-11-15）

1. 管理台入口统一为 http://127.0.0.1:8765/（后端直接服务最新 admin/static，no-cache）；
   nginx 的 /ngxdb/admin/ 改为跳转页，不再单独维护静态拷贝；
2. 版本徽标自动显示实际加载的 admin.js 版本（取自 script src），避免“版本还是旧值”的困惑；
3. 外键编辑行增强：外键字段下拉随“字段”增删/改名自动联动；新增“模式”下拉与主表下拉
   两级联动（先选模式再选该模式下的主表）。admin.js v26。

---
## 修复：aaa 模块无法打开的两层原因（2026-11-15）

1. **nginx 热重载失败**：CREATE EXTENSION postgres_fdw 时其 C 函数（如
   postgres_fdw_disconnect）落在了 public schema，nginx postgres 模块热重载扫描函数表时
   因“declares 1 args but has no argnames”而失败（keeping old function table），导致
   新发布的 aaa 函数无法经 nginx 访问（前端提示无法连接服务器）。
   **修复**：ALTER EXTENSION postgres_fdw SET SCHEMA pg_catalog，热重载恢复成功。
2. **外键 join 类型不匹配**：aaa.bbb(varchar) 外键引用 public.city.cid(integer)，
   生成的 join `t.bbb=fj1.cid` 报 operator does not exist。
   **修复**：模板改为 `t.bbb::text=fj1.cid::text`（写入校验同样 ::text 比较），
   已发布的 aaa 四个函数已同步替换并热重载；aaa_query 实测正常（errorcode 0）。

---
## 管理台 v30：数据表列表/修改表增加错误显示（2026-11-15）

loadTables/renderTables 包裹 try-catch：加载或渲染失败时在表清单区域直接显示
“表清单加载失败/渲染失败: 原因”，不再静默失败导致页面缺按钮/缺数据。

---
## 修复：误删 aaa 模块权限导致“无此权限”（2026-11-15）

清理测试权限节点时误按 actionid 范围（200-283）删除，连带删除了 aaa 模块的操作权限
（actions 206-211 与 sysaction 操作级节点）。已按生成模板的原始定义恢复：
- sysinfo.sysaction：206 新增/207 修改/208 合并/209 删除/210 恢复/211 查询（idpath
  100.9003.9115.20x，upid=9115）；
- sysinfo.actions：206-211 对应 sysinfo/aaa_add|edit|merge|del|undel|query；
- 与 aaa 各函数 check_login 的权限点一致。热重载后 aaa_query 实测 errorcode 0。
**教训**：清理测试权限必须按 sysactionname 精确匹配，禁止按 id 范围删除。

---
## 管理台 v28：外键录入体验增强（2026-11-15）

- 选定主表后自动回填“主字段=主表主键”（只读）并计算外键字段应有类型
  （单选=主键类型，多选=主键类型[]），同步到字段列表（类型下拉自动匹配/追加选项）；
- “显示字段”改为多选下拉（选项=主表列，含中文注释提示）；
- 创建/保存时按外键配置强制校准字段类型（单选=主键类型，多选=数组类型），
  修改表时自动生成带 USING 的类型转换（varchar 逗号文本 ↔ 数组安全互转）；
- 生成模板支持数组外键列（参数 varchar、写入转数组、query array_to_string + any 联查）；
- 修复 select_list 模板的 format 转义（'{}'）导致的 IndexError。

---
## nginx 插件：执行错误返回标准 JSON（2026-11-15）

- `postgres/ngx_http_postgres_module.c`：函数执行出错（PG 报错）时不再返回原始
  `ERROR: ...` 文本（前端 JSON.parse 失败显示“无法连接服务器”），改为统一返回
  `{"errorcode":9998,"message":"<PG错误主消息>","info":null}`（HTTP 200、
  application/json），errorcode 落在 100~10000 系统错误段；
- 错误消息做 JSON 字符串转义（引号/反斜杠/控制字符），日志降为 ERR 级别并使用固定
  格式串（修复原 emerg 日志把错误消息当格式串的隐患）；
- 主响应路径 Content-Type 由 text/html 改为 application/json;charset=utf-8；
- 待执行 `sudo bash rebuild_nginx.sh` 重编译并重启 nginx 后生效。

---
## 生成页面：前端输入类型校验（2026-11-15）

- `gen_module.py` 生成 modules.js 时在 fields 上输出数据库类型标记 `ty`
  （如 integer、varchar(32)、integer[]、timestamp），供前端校验使用；
- ngxdb 前端 `app.js` 新增 `fldTypeErr(ty,v)`：提交前按字段数据库类型校验输入
  （整数/数字/布尔/日期/时间戳/时间/varchar 长度/数组逐项），不符则提示
  “字段名 应为整数/长度不能超过 N/第N项应为整数”等并**阻止提交**；
  无 ty 标记的旧模块不受影响（跳过校验，向后兼容）；
- 已用 aaa（integer[]/text）重新生成验证：modules.js fields 含 ty，
  非法输入（如 aaa=1,x）前端即拦截，不再打到后端报 SQL 错误；
- 前端版本号升至 `app.js?v=71` 并同步发布目录。

---
## aaa 外键选择器修复（2026-11-15）

- 根因：fk_meta 中 aaa.aaa/aaa.bbb 两条外键的 master_module 为空，生成的 modules.js
  fields 无 tp/lp 标记，前端不渲染外键选择按钮；
- 修复：补 master_module（aaa→sys_dict 多选、bbb→city 单选）并重新生成发布，
  aaa 新增/修改页两个外键字段出现“选择”按钮（列表选择器，多选逗号累积）；
- 增强：openListPicker 加载失败（无主表查询权限）时关闭弹窗、提示
  “无该主表查询权限，请手动输入”并解除输入框只读（降级手输）；
- 前端版本号升至 app.js?v=73 并同步发布目录。

---
## aaa 页面按钮消失修复 + 权限点漂移根修（2026-11-15）

- 现象：aaa 新增/修改/删除按钮全部不显示；
- 根因：`suggest_ids()` 每次按 max(sysactionid)+1 分配权限点，aaa 多次发布后
  函数 check_login 的权限点漂移至 236+/242+/248+，与用户实际持有的 206-211 不一致，
  前端按 modules.js 的 ops 判断按钮显隐 → 全部隐藏；
- 根修：`build_def` 在未显式指定 actions 时，优先按 actionurl 复用该表已发布的
  操作权限点（sysinfo.actions），不再每次发布重新分配；
- aaa 已重新生成发布：函数 check_login 与 modules.js ops 统一回 206-211
  （add=206/edit=207/merge=208/del=209/query=211），HTTP 实测 add/query 权限校验通过；
- 教训补充：admin 进程的 cmdline 是 `python3 app.py`，用 `pkill -f 'admin/app.py'`
  杀不掉它，应使用 `pkill -f 'python3 app.py'`。

---
## 外键选择器多选增强（2026-11-15）

- openListPicker 支持 multi 模式：多选外键（如 aaa.aaa→sys_dict）在弹窗内点击行
  切换选中态（高亮），底部“确定”一次性带回所有选中值（逗号累积去重），
  不再是选一个就关弹窗；打开时预标记已选值；
- 单选外键行为不变（点行即选即关）。

---
## 多选外键确定后保留现有值（2026-11-15）

- 修复：编辑时 tp 输入框未初始化 data-value，多选“确定”后累积从空开始，
  丢掉已有值；现打开表单即把现有值写入 data-value，确定后在其基础上累积；
- 多选显示改为与提交值一致的 id 串（不再与名称标签混排），避免显示错乱。

---
## 外键显示列展示（2026-11-15）

- 生成模板：modules.js 的外键字段输出 disp（显示列名串，如 code,dictname）；
- 列表页：cols 本就含外键联查列（aaa_code/aaa_dictname/bbb_cid），随查询返回展示；
- 表单弹窗：外键输入框回显联查列值（名称）而非纯 id；多选“确定”后按
  ids/名称同步累积显示（如 `sdff / xxx`），提交仍为 id 串；
- app.js 版本 v79，发布目录已同步。

---
## 外键列名与显示优化（2026-11-15）

- 列表外键联查列的列名改为“主表字段注释+外键字段注释”
  （如 aaa编码 / aaa名称 / bbb市id），不再是单独的外键字段注释；
- 列表中多选外键列优先显示联查名称聚合值（如 aaa 列显示 aaa_code 的值），
  避免数组/ID 原样展示；
- 弹窗多选确定后的显示改为逗号分隔（与提交值格式一致）。

---
## 列表选择器弹窗按外键显示列（disp）渲染（2026-11-16）

- 修复：aaa 新增/修改弹窗中点“选择”（aaa 列，外键→sys_dict，disp=code,dictname）
  弹出的列表选择器只显示写死的“编号/名称”两列——缺少“编码”列，且列名固定
  不随主表变化（如 bbb→city 的 disp=cid，第二列也叫“名称”）；
- `openListPicker` 新增 disp 参数：弹窗列 = 编号(主键) + disp 各列，列名取
  主表模块的中文标题（sys_dict→编码/名称；未配置标题时用字段名）；
  未配置 disp 的旧模块回退为主表第一个含 name 的列，行为与之前一致；
- 选中回显文本改为取第一个有值的显示列（与表单回显、列表页联查列展示一致）；
- 生成模板 `gen_module.py` 的 disp 注释同步更新（表单回显 + 选择器弹窗列）；
- app.js 版本升至 v81，发布目录已同步。

---
## 登录页新增“记住密码”功能（2026-11-16）

- 登录页（login.html）密码框下新增“记住密码”勾选：勾选并**登录成功后**，
  账号+密码以 Web Crypto AES-GCM（随机 256 位密钥 + 12 字节 IV，无 AAD，
  密文=ct‖tag）加密存入本机 localStorage（`ngxdb_rmk` 密钥 / `ngxdb_rpc` 密文，
  base64(iv).base64(ct) 拼接），下次打开登录页自动回填账号与密码框；
- 安全边界：口令不落明文，可防直读/截图/日志泄露；但密钥与密文同存本机，
  防不了本机恶意脚本——共享电脑、等保三级（要求双因素鉴别）场景请勿勾选，
  页面已附提示文字；服务端口令仍为 blowfish 单向哈希（不接触明文），
  传输仍走 HTTPS POST，符合等保对鉴别信息的存储/传输要求；
- 仅登录成功后写入（确保保存的是有效凭据）；取消勾选即清除已存凭据；
  解密失败（数据被篡改/密钥更换）自动清除坏数据；密码错误不保存；
- 非 HTTPS/无 Web Crypto 的环境自动隐藏该选项（降级不启用）；
- 纯前端改动，不涉及数据库函数，无需热重载；login.html 已同步发布目录。

---
## 管理台部署脚本不再发布静态页到 nginx（2026-11-16）

- `admin/deploy.sh` 删除“静态页发布到 nginx”步骤：管理台页面由后端直接服务
  （`admin/static`，no-cache），访问入口仅 `http://127.0.0.1:8765/`（仅本机）；
- 脚本增加清理逻辑：删除 nginx 部署目录中的管理台历史发布残留
  （`$NGXDB_DEPLOY_DIR/admin`，带空值/根路径保护）；
- 验证段去掉 nginx 静态页检查，仅保留 `/api/ping` 自检；
- `NGXDB_DEPLOY_DIR` 环境变量保留传递（app.py 发布**业务模块**到部署目录仍需要）；
- 同步更新文案：`admin/static/index.html` 系统信息页提示、`admin/README.md`、
  `README.md`、`docs/DEVELOPMENT.md`、`install.sh`（注释与两处就绪提示）；
- 已实测：8765 直连 200；`https://127.0.0.1/ngxdb/admin/` 404（残留已清理）；
  业务前端 `/ngxdb/` 不受影响。

---
## 管理台：树表默认结构列标必填；外键模式/主表下拉显示全部模式（2026-11-16）

- **树表默认字段必填**：新建树表时自动创建的 4 个结构列（upid/idpath/idlevel/idcount）
  在弹窗“树表结构列”区以“只读 + 必填勾选（选中）”展示，明确其为必填结构字段
  （`addFieldRow` 支持 `opts.req`；locked 行的必填勾选同时置灰不可改）；
- **外键“模式”下拉只显示 erp 的修复**：`fkFillSchemas` 原从 `TABLES`（数据表页面
  当前模式筛选结果）取模式——页面筛选 erp 后外键模式下拉就只剩 erp。现改为使用
  `/api/schemas` 的全量模式列表（新增 `SCHEMAS` 缓存），接口不可用时才退回旧逻辑；
- **外键“主表”下拉同步修复**：`fkFillTables` 改为按所选模式实时调用
  `/api/tables?schema=` 拉取该模式全部表（不再依赖受筛选的 TABLES），
  并加请求序号防止快速切换模式时的过期响应覆盖；
- 新建表弹窗的“模式”下拉本就来自 `/api/schemas`（全量），实测返回
  conn/erp/gm/operator/public/sysinfo 6 个模式；
- admin.js 版本 v30→v31（后端 no-cache 直服，刷新页面即生效）。

---
## 管理台主页面：操作列移到第一列（2026-11-16）

- 数据表管理（主面板）表格列序由“表名/模式/注释/大小/操作”调整为
  **“操作/表名/模式/注释/大小”**：表头（index.html）与行渲染（admin.js renderTables）
  同步调整，结构/修改、生成模块、删除三个按钮置于每行首列；
- admin.js 版本 v31→v32（后端 no-cache 直服，刷新页面即生效）。

---
## 管理台：结构/修改弹窗按真实约束回显“必填”勾选（2026-11-16）

- 修复：结构/修改弹窗（showAlter）中字段行的“必填”勾选框此前全部未勾选；
- 现按真实约束回显：数据库非空列（`is_nullable=NO`，含主键）自动勾选必填；
  树表结构列（upid/idpath/idlevel/idcount）与新建树表弹窗保持一致，强制勾选必填
  （只读置灰）；可空业务列不勾选；
- 数据来源为 `/api/table` 返回的 `is_nullable`（table_columns 本就查询该列，后端无改动）；
- 已用树表 erp.atree 实测回显结果：atreeid（主键）+ upid/idpath/idlevel/idcount 勾选
  必填，业务列 kkk（可空）不勾选；
- admin.js 版本 v32→v33（刷新页面即生效）。

---

## 删表同步清理已发布页面；修复树表模板缺引号导致模块不可用（2026-11-16）

**问题 1：删除表后，已发布到前端的模块页面残留。**
- 修复：`admin/app.py` 新增 `unpublish_module(table)`——从项目与部署目录两份 modules.js
  中移除该表的生成块（含父子表 SUBTABLECFG 段），并 `bump_index()` 升级资源版本号；
  `drop_table` 删表成功后同步调用（失败不阻断删表）；
- 同时清理权限残留：`with_funcs` 时按 6 个操作后缀**精确**删除 `sysinfo.actions` 中
  `schema/table_query|add|edit|merge|del|undel` 行（不用 LIKE 避免误删 `atree2` 类表），
  与 sysaction 节点删除同一事务；并清理 `public.fk_meta` 中该表作为引用方/被引用主表的记录。

**问题 2：点击新发布的树表 atree，前端提示"该功能没有发布"。**
- 根因：`tools/gen_module.py` 树表模板 `{f:upid,tp:'...'}` 的 `upid` **缺引号**，
  modules.js 运行时抛 `ReferenceError: upid is not defined`，atree 的 `P(...)` 调用
  （实参求值阶段）失败，模块从未注册——菜单点击后自然找不到模块；
- 修复：模板改为 `{f:'upid',tp:'...'}`；并修正项目与部署目录两份已发布 modules.js 中
  atree 的该行（`modules.js?v=79→80`）。经 nginx 线上拉取与磁盘 diff 确认一致。

**端到端验证**（临时表 erp.zz_deltest：建表→发布→删表全流程）：
- 发布后两份 modules.js 均出现 `P('zz_deltest',...)` 块；
- 删表后：两份 modules.js 残留 0、index.html 版本自动 +1、erp 表列表无 zz_deltest、
  权限树无 9118/260-263 残留、6 个生成函数随事务 DROP、热重载正常（functions=91）。


---

## 数据清理：删除 sysaction 中 actionid 失配的残留权限节点（2026-11-16）

**问题**：`sysinfo.sysaction` 中存在 42 条 `actionid` 在 `sysinfo.actions` 表中无对应行的记录
（sysactionid 212–253，全部为 aaa 模块的"新增/修改/合并/删除/恢复/查询"操作节点，挂在 9115 下）。

**成因**：aaa 模块历史上重复发布 7 次——每次发布都为新 sysaction 节点分配新 actionid，
而 actions 表按 actionurl 判重不再插入，导致这批节点失配（后续发布已增加按 actionurl
复用权限点的逻辑，见 2026-11-15 记录，不会再新增此类残留）。

**删除前安全核查**：
- aaa 模块前端实际引用的权限点为 **206–211**（modules.js `{add:206,edit:207,merge:208,del:209}`），
  且 actions 表中 206–211（sysinfo/aaa_add/edit/merge/del/undel/query）齐全——残留节点不被引用；
- 42 条记录均无子节点（不影响权限树结构）；rolepermission/operpermission 对 212–253 引用为 0。

**执行**：事务内
`DELETE FROM sysinfo.sysaction s WHERE s.actionid IS NOT NULL AND s.actionid<>0
AND NOT EXISTS (SELECT 1 FROM sysinfo.actions a WHERE a.actionid=s.actionid);`
→ `DELETE 42`，sysaction 总行数 149→107。

**删除后验证**：失配记录 0；aaa 在用节点 206–211 完好；权限树接口返回 107 个节点与表行数一致，
已删区间无出现；rolepermission/operpermission 无悬挂引用。


---

## 修复：会话超时跳转的登录页是旧版（无"记住密码"）（2026-11-16）

**现象**：登录超时后跳转到的登录页与初始登录页不一致——没有回填已保存的密码，
也没有"记住密码"选项。

**根因**：nginx 对 login.html 等静态页**未发送任何 Cache-Control 响应头**（仅
Last-Modified/ETag），浏览器按启发式策略缓存。浏览器缓存键为完整 URL——
"记住密码"功能上线前经历的"超时跳转 `login.html?redirect=...`"把**旧版登录页**
按该带参 URL 缓存了下来；之后超时跳转命中旧缓存条目（不再向服务器校验），
而直接打开的 `login.html`（无参数）则拿到了新版，于是两个页面不一致。
（另核实：根路径 `/login.html` 为 404，前端仅 `/ngxdb/login.html` 一个入口，文件已同步。）

**修复**（纯前端，core.js / login.html）：
- core.js 新增 `withTs(url)`：在 URL 的 query 部分追加 `_ts=毫秒时间戳`
  （插入点保持在 `#hash` 之前），每次跳转 URL 唯一，强制浏览器向服务器取最新文档；
- `logout()` 跳转登录页、login.html 登录成功回跳（redirect 或 index.html）
  均改经 `withTs()` 处理；
- 资源版本：login.html 的 core.js v7→v8；index.html 的 core.js v81→v82；
  三个文件（core.js/login.html/index.html）已同步部署目录，经 nginx 线上拉取确认生效。

**说明**：浏览器本地可能仍残留旧版登录页缓存条目（不受服务端控制），手动
`Ctrl+F5` 一次即可清除；此后超时跳转恒取最新登录页。


---

## 修复：aaa 多选外键编辑/保存后显示不正确（2026-11-16）

**现象**：aaa 页面编辑数据时，修改 aaa 列（→sys_dict 多选外键）选择返回后表单显示不正确；
点保存后主页（列表）显示的 aaa 列也不正确。

**根因**（app.js 多选外键回写逻辑，两处缺陷叠加）：
1. **无法移除已选值**：多选弹窗"确定"时只对仍处于勾选态的行**逐行**回调 onOk，
   而表单侧 done 回调是纯**累积**逻辑（只增/改，不删）——被取消勾选的旧值
   永远保留在 dataset.value 中（想把 `1,2` 改成 `1,3`，实际提交 `1,2,3`）；
2. **显示文本错乱**：回显 labs 用 `' / '` 分割、回写却用 `','` 拼接，分隔符不一致；
   labs 个数少于 ids 时用 **id 数字补位**，表单显示混入裸 id（如 `A,2,C`）。
   保存后列表经 query 联查显示的是错误 id 集合的聚合结果，故主页显示也不正确。

**修复**：
- `openListPicker` 多选模式"确定"改为**一次性**收集全部选中行
  `[{id,label},...]` 整体回调 `onOk(null,null,selRows)`（整体替换语义，支持增/减/清空）；
- 表单 `done` 回调新增 `all` 参数：多选时以弹窗最终选中集合**整体重置**
  ids 与显示文本；逐行累积逻辑保留为兜底（树选择器路径），分隔符统一为逗号。

**验证**（Python 模拟交互流程，5 场景全过）：换值 `1,2→1,3`（显示 A,B→A,C）、
全取消清空、追加、兜底累积、单选外键均正确；app.js v84→v85，已同步部署目录
并经 nginx 线上确认。纯前端改动，刷新页面即生效。


---

## 修复：前端菜单出现两个 atree；atree 页面报"服务器返回了无法解析的内容"（2026-11-16）

**现象**：ngxdb 前端左侧菜单出现两个"atree"：一个点击进入空占位页（此功能未提供），
另一个点击报"服务器返回了无法解析的内容"。

**根因**（两个独立问题叠加）：
1. **菜单重复**：发布 atree 时权限挂载点选在了名为"atree"的目录节点 9116 下，
   发布器在其下创建功能节点 9117（modules.js 引用 menu=9117）。左侧菜单渲染时
   无模块的目录节点 9116 也被渲染成可点菜单项（空占位页）→ 出现两个同名入口；
2. **接口返回非法 JSON**：atree 建表后表结构变更（删除了 kji/lll 列），但生成函数
   与 `public.fk_meta` 中的外键登记未同步——`atree_query` 等函数体仍按残留的
   fk_meta 联查 `t.kji`/`t.lll`，执行即报 `column t.lll does not exist`；
   nginx postgres 模块对函数执行异常输出的错误 JSON 未对消息转义
   （`"message":column t.lll does not exist"`），前端 `res.json()` 解析失败，
   显示为"无法解析的内容"，掩盖了真实错误。

**修复**：
- 清理 fk_meta 中 atree 已失效的外键登记（2 条），用当前表结构**重新发布 atree**
  （重建 5 个函数，实测 `atree_query` 恢复 errorcode 0 合法 JSON；modules.js 幂等更新）；
- `app.js` renderNavFromRows 重构为递归渲染：**无模块的目录节点渲染为可折叠子菜单**
  （nav-dir：图标+名称+箭头，**点击标题展开/收起下级菜单**，默认展开），其子级
  （功能节点 9117 的 atree）嵌套显示在目录内——消除重名重复入口的同时保留
  权限树的层级导航；叶子功能无模块仍显示空占位页；
- `suite.css` 新增 .nav-dir 系列（标题 hover、箭头旋转、收起时隐藏子列表、缩进）；
- `admin/app.py` 补通用防护：**修改表删列（update_fields / alter dropcol）后同步
  删除该列的 fk_meta 登记，改列名（renamecol）同步更新 fk_column**——避免今后
  表结构变更后 fk_meta 残留导致生成函数再次引用已删列。

**验证**：atree_query 经 nginx 实测 errorcode 0；菜单渲染模拟（9116 → nav-dir 折叠
容器、内含 9117 的 atree 功能项，无空占位项）；权限管理页权限树数据仍完整
（9116/9117 均可管理）；admin/app.py 语法通过、服务已重启；线上 app.js?v=89、
suite.css?v=84 确认生效。
（注：本条目初稿的目录处理方案为"透传"，按需求修正为"可折叠目录"。）


---

## 修复：树表生成模板硬编码 isused/systemid、upid=0 误判"上级不存在"（2026-11-16）

**现象**：atree（树表，无 isused/systemid 列）新增记录报"服务器返回了不可解析的数据"。

**根因**（`tools/gen_module.py` 树表 SQL 模板两处缺陷）：
1. 树表函数的上级校验、循环定义检查、有下级禁删、`tree_recalc` 递归重算均**硬编码**
   `isused<>0` 与 `systemid` 字段（自 sysorg 抄来）——建模树表可以没有这两列，
   函数一执行即报 `column "isused" does not exist`（经 nginx 模块异常输出为坏 JSON）；
2. `_parent_check` 仅在 `p_upid is not null` 时校验上级存在——**upid=0（根节点约定值）
   也会触发 100014"上级不存在"**，导致根节点无法新增。

**修复**（`tools/gen_module.py`，plpgsql 与 plpython3u 模式共用）：
- `tree_recalc`：`isused<>0` 过滤（2 处）与 `systemid`（CTE 列/join 条件/计数分组）
  按 `has_isused`/`has_sysid` 条件化生成；
- `_parent_check`/`_loop_check`/`gen_del`（有下级禁删）：`isused<>0` 按 `has_isused` 条件化；
- `_parent_check` 增加 `p_upid<>0` 判断（0=根节点，不校验上级）；
- `gen_del` 父子表"有子记录禁删"的子表 `isused<>0` 同步按 `child_fields` 条件化。

**验证**：重新发布 atree 后——库内 `atree_add` 函数体 isused/systemid 引用为 0；
psql 直调与经 nginx 实测新增根节点（upid=0）、子节点（upid=1）、`atree_query`
均返回 errorcode 0；测试数据已清理。


---

## 树表创建时自动内置 id=100 的默认根节点（幂等去重）（2026-11-16）

**需求**：树表创建后默认带一个 id=100 的根节点，且重复执行不重复插入（幂等去重）。

**实现**（建表 DDL 末尾附加，两条路径同步修改）：
- `tools/gen_module.py`：新增 `tree_root_insert(d)`，`gen_create_table` 在树表 DDL 末尾附加——
  `INSERT INTO <表> (pk,upid,idpath,idlevel,idcount[,isused][,systemid]) SELECT 100,0,'100',1,0[,1][,<systemid>]
  WHERE NOT EXISTS (SELECT 1 FROM <表> WHERE pk=100)`；isused/systemid 按表实际结构（has_isused/has_sysid）附带，
  systemid 取 def 的 systemid（默认 100）；
- `admin/app.py` `create_table`：管理台"新建表"的树表 DDL 同步附加同样的幂等根节点 INSERT
  （isused/systemid 按用户勾选的 isused_col/sysid_col）。

**验证**（端到端，经管理台 API 新建 zz_treetest 树表）：
- 建表后表内自动出现且仅一条 id=100 根节点（upid=0、idpath='100'、idlevel=1、idcount=0、
  isused=1、systemid=100）；
- 重复执行该 INSERT：第二次 0 行（WHERE NOT EXISTS 去重生效），id=100 仍为 1 条；
- 生成器单测（无 isused/systemid 的树表）：DDL 正确附带根节点 INSERT 且不含这两列；
- 已存在的树表 erp.atree 亦已补插 id=100 根节点（幂等执行两次，仅 1 条）；
- 测试表已删除清理。


---

## 树表页面：点击树的节点同步选定当前记录（2026-11-16）

**需求/问题**：树表页面（左树右表布局）此前只有点击右栏表格行才会"选定当前记录"
（`_state.sel`）；点击左树节点仅切换右栏的显示范围（`_state.node`），不选定记录。
根节点只出现在左树、永远不会出现在右栏表格里——导致根节点永远无法被"修改/删除"
（这些操作按钮依赖已选定的当前记录）。

**修复**（`app.js`，树表通用逻辑，app.js v93→v94）：
- `renderTree` 的树节点点击：设置 `_state.node` 的同时，按节点 idpath 在数据行中
  匹配并**同步设置 `_state.sel` 为该节点记录**——点击树的节点即选定当前记录；
- `renderRows` 表格行生成时**回显选中态**：与 `_state.sel` 匹配的行自动带
  `selected` 高亮（此前只有点击行时才加，切换树节点后高亮丢失）。

**验证**：模拟点击根节点（idpath='100'）/子节点（idpath='100.101'）均正确设置
当前记录；表格行选中态回显正确；app.js 已同步部署目录并经 nginx 线上确认
（app.js?v=94）。生成模板（modules.js）无需变动——树表交互为前端通用逻辑。


---

## 树表页面：点击表格行时清除左树节点的选定高亮（2026-11-16）

**需求**：树表页面（左树右表）点击左树节点会选定当前记录（树节点高亮）；点击
右栏表格行选定记录时，左树节点的选定高亮应同步消失——当前记录的来源在两者之间
互斥切换。

**修复**（`app.js` renderRows 表格行点击，app.js v94→v95）：点击表格行后移除
`#treePane .tnode.on` 高亮、并将 `_state.node` 置空（当前记录改由表格行选定）；
再次点击树节点时恢复树节点高亮与选定（既有逻辑）。

**验证**：交互模拟通过——点表格行后表格行选中、树高亮清空；再点树节点后树高亮
恢复、表格选中清除；app.js 已同步部署目录并经 nginx 线上确认（app.js?v=95）。


---

## 管理台：修复"修改表"弹窗外键行重复累积（2026-11-16）

**现象**：修改 atree 结构时点击"增加外键"后出现多个外键行；取消再重新打开弹窗，
重复的外键行仍在（越开越多）。

**根因**：`admin.js` 的 `showAlter`（修改表弹窗）打开时从 `public.fk_meta` 读取
外键配置后逐条 `addFkRow` **追加**到 `#nt-fk-list` 容器，但**从未清空该容器**——
上次打开时添加/读取的外键行全部残留，与本次读取的配置叠加，越开越多；
保存时 `collectFks` 还会把残留的重复行一并提交。

**修复**：`showAlter` 在读取外键配置前先 `$('nt-fk-list').innerHTML = ''` 清空容器
（"新建表"入口原本就有清空，本次补齐"修改表"路径）。admin.js v33→v34，
经管理台后端直服线上确认生效。

**核查**：atree 当前 fk_meta 无重复残留（重复行未成功保存过，唯一索引
fk_meta_uniq(fk_schema,fk_table,fk_column) 也会拦截同列重复插入）。


---

## 修复：重复生成导致权限树出现两套 atree 节点、旧节点点击显示"该功能页面暂未提供"（2026-11-16）

**现象**：atree 重新生成发布后，在权限管理页面点击 atree（旧节点）提示"该功能页面
暂未提供"（无此功能页面）。

**根因**：`gen_sysaction` 生成的权限节点 INSERT 为 `ON CONFLICT (sysactionid) DO NOTHING`，
而菜单节点 id（menu_action）由管理台每次"生成"时自动分配**新 id**——重复生成/发布时
actions 具体权限按 actionurl 判重复用了旧 id，但**菜单/操作节点在权限树上新建了第二套**
（9118→260-265），modules.js 的 menu 随之漂移到 9118；旧套（9116→9117→254-259）成为孤儿——
点击旧节点无对应模块页面 → 空占位提示。

**修复**：
- `tools/gen_module.py` `gen_sysaction`：INSERT 改为 `ON CONFLICT (sysactionid) DO UPDATE`
  （更新 upid/idpath/idlevel/idcount/名称/actionid/systemid）——重复发布时节点**跟随本次
  挂载点自愈**，不再残留旧位置孤儿（plpgsql/plpython3u 共用）；
- `admin/app.py` `build_def`：menu_action **复用同分组下同名的已有功能节点**（按
  upid+sysactionname+actionid is null 查询）——重复生成时菜单 id 保持稳定不漂移；
- 数据清理：删除孤儿节点 9116/9117 与失配的 260-265（无 rolepermission/operpermission
  引用，安全）。

**验证**：重新发布 atree 后权限树仅剩一套：9118(atree，挂 9003) → 254-259（操作，
已自愈挂到 9118 下，idpath=100.9003.9118.254…）；modules.js atree 块 menu=9118、
ops=254-257 与 actions 表一致；前端左树右表与权限管理页点击 atree 均进入模块页面。


---

## 生成模板：已有权限信息直接载入，不再每次生成新权限（2026-11-16）

**需求**：生成模块时若该表已有权限信息，应**载入复用**而不是重新生成——避免每次
生成都产生新的权限节点、菜单 id 每次变化。

**实现**（`admin/app.py` `build_def`，三级复用）：
1. **跨分组载入**：按本表任一操作的 actionurl（如 `erp/atree_add`）反查 sysaction 中
   的操作节点，其 upid 即本表已发布的菜单节点——直接载入该 id（即使本次选择了
   不同的挂载点，发布时节点也会经 `gen_sysaction` 的 ON CONFLICT DO UPDATE 迁移过去）；
2. 次选：同分组下同名的已有功能节点（actionid is null）；
3. 均无（首次建模）才自动分配新 id。

配合此前 `gen_sysaction` 的 ON CONFLICT DO UPDATE，重新生成/发布时权限树节点
**集合与 id 完全稳定**，仅挂载位置/名称随本次配置更新。

**验证**：atree 连续两次生成（挂载点 9003→9113、menu_action 分别传新分配的
9200/9300）——两次生成的 modules.js 均 `menu=9118`（载入复用，未采用新 id），
sysaction 均为 7 条幂等 INSERT；库内 atree 节点保持一套（9118+254-259）无新增。


---

## 前端：权限变更后引导重新登录而非跳回默认页（2026-11-16）

**现象**：权限树节点迁移（如 atree 菜单 9117→9118）后，点击 atree 提示
"无权限访问该模块"并被跳回默认页。

**根因**：登录时后端一次性下发用户权限点列表（ACTIONS，存于本机）。权限树变更
（节点迁移/新增）后，浏览器里的**旧登录态**仍持有变更前的 ACTIONS（含旧节点 id、
不含新节点 id）——前端 `hasPerm(新菜单id)` 失败。后端 `gm.check_login` 本身按
"权限树上存在该权限点"判定（对新有效登录放行），与前端旧列表不一致。

**修复**（`app.js` route()，app.js v98→v99）：`hasPerm(m.menu)` 失败时改为
`logout('您对该功能的访问权限已变更，请重新登录')`——清除旧登录态并跳转登录页
（携带 redirect 原路回跳，配合"记住密码"回填可一键登录返回原页面），
不再跳去无关的默认页造成困惑。

**验证**：新登录实测 actions 列表已含迁移后的菜单 id（9118）与操作权限点
（254-257）；修改区域人工复核正确；app.js 已同步部署目录并经 nginx 线上确认
（app.js?v=99）。


---

## 修复：多选外键显示列为数值型时 query 报"不可解析"（2026-11-16）

**现象**：atree 重新生成后，点击页面提示"服务器返回了不可解析的数据"。

**根因**：atree 的多选外键 bbb→sys_dict 的显示列配置为 `dictid,code`，其中
`sys_dict.dictid` 为 **bigint**。生成模板 `select_list` 的多选外键聚合
`string_agg(m.<显示列>, ...)` 对非文本列未做 `::text` 转换——
`string_agg(bigint, ...)` 无此函数签名，query 执行即抛异常
（nginx 模块异常输出为坏 JSON → 前端"不可解析"）。

**修复**（`tools/gen_module.py` select_list）：聚合列统一加 `::text`
（`string_agg(m.<显示列>::text, ...)`，文本列不受影响），plpgsql/plpython3u
共用该路径。

**验证**：重新发布 atree 后实测——`atree_query` 返回 errorcode 0（含
bbb_dictid/bbb_code/ccc_city 联查列，4 行数据）；`atree_add` 以合法外键值
新增成功（errorcode 0）；探针数据已清理。


---

## 修复：树表外键显示/修改异常——master_module 未登记导致表单缺外键标记（2026-11-16）

**现象**：atree（树表）编辑时外键列（bbb→sys_dict 多选、ccc→city）显示为原始值、
无法使用选择器修改，与单表页面（aaa）的外键处理不一致。

**根因**：管理台"修改表"配置外键时"主表模块id(可空)"留空 → `public.fk_meta.master_module`
为空 → 生成模板 `gen_modules_js` 按 `if fk['module']` 跳过 `tp/lp/multi` 标记的生成——
前端表单字段无 `tp`（数据源模块）、`lp`（列表选择器）、`multi`（多选）标记，
外键回退为普通文本输入框（显示原始 id/数组文本，无法选择修改）。
（单表 aaa 当初是手动补登了 master_module 才正常的。）

**修复**：
- `tools/gen_module.py` `get_fks`：`master_module` 未登记时**默认取主表表名**
  （项目约定模块 id=表名，如 sys_dict/city/atree），所有表的生成兜底生效；
- `admin/static/admin.js` `addFkRow`：选主表后若"主表模块id"为空**自动带出主表表名**
  （可手动覆盖），后续配置不再依赖手工登记；
- 数据修复：补齐 atree 两条外键的 master_module（bbb→sys_dict、ccc→city）并重新发布。

**验证**：发布后 modules.js 的 atree 表单字段带齐全标记
（bbb: `tp:'sys_dict',lp:1,multi:1`；ccc: `tp:'city',lp:1`），前端编辑回显联查文本、
点"选择"弹出列表选择器（bbb 多选勾选/ccc 单选）、保存提交 id 串——与单表页面
（aaa）的外键处理完全一致；线上（经 nginx）与磁盘文件一致。


---

## 树表页面：右栏展示当前节点自身 + 仅在表格中选定记录（2026-11-16）

**需求**：树表的根信息在页面看不到（点左树节点后右栏只显示其直接下级，根节点
自身从右栏消失，无法查看/修改）；同时树的节点不再承担"选定当前记录"职责，
记录的选择统一在右栏表格中进行。

**实现**（`app.js` + `suite.css`，app.js v101→v102、suite.css v93→v94）：
- `renderTable` 树表过滤：选中树节点后，右栏 = **当前节点自身行（首行）+ 其直接下级**；
- 当前节点自身行加 `cur-node` 类与"当前"徽标（首列），与下级行区分；
- 撤销此前"点击树节点同步选定 `_state.sel`"的逻辑——树节点仅负责层级导航
  （切换右栏展示范围），**选定记录只发生在右栏表格行点击**；
- 表格行点击仍会清除左树节点高亮（此前已实现），交互闭环。

**验证**：逻辑模拟——点根节点（idpath=100）右栏首行为根自身、其后为直接下级
（不含孙级）；未选节点时显示顶级；树点击不清除表格已选记录。app.js/suite.css
已同步部署目录并经 nginx 线上确认（app.js?v=102、suite.css?v=94）。


---

## 数据修复：atree 默认根节点 100 被误挂深层导致"看不见"（2026-11-16）

**现象**：编辑节点 4 时上级选不了 100（提示不能选自己和下级）；改选 fff 保存后，
刷新页面 100 节点"不见了"。

**原因**：默认根节点 100 此前被挪到了 8 之下（idpath=4.5.8.100，第 4 层）且名称为空——
- 编辑 4 时选 100 被拒是**正确的循环保护**（当时 100 在 4 的子树里，4 挂到 100 下会成环）；
- 100 位于第 4 层且页面刷新后默认只显示顶级/未展开层，因此在页面上"看不见"。

**修复**：恢复 100 为根节点（upid=0、idpath='100'、idlevel=1、idcount=0），
并全表重算 idpath/idlevel/idcount。修复后结构：
- 100（根）、4（根，ssd）→ 5(ffff) → 8(ddd)；4 → 6(ddd)。

**说明**：树表编辑的循环保护（100017）行为正确——不能把节点挂到自己或自己的
下级之下；需要调整此类层级时，应先把下级节点移出子树。


---

## 树表左树节点统一显示：主键 | 第一个业务字段值（2026-11-16）

**需求**：树表页面左树（层级导航）节点显示的信息不统一（nameOf 按"name 列/第一列"
取值，不同节点来源混乱）。统一为：**主键 + 分隔符（": "）+ 创建时的第一个业务字段值**。

**实现**（`app.js`，app.js v102→v104）：
- 新增 `firstBizField(m)`：取生成模块时 fields 的第一个**非结构列**字段名
  （排除主键/isused/systemid/upid/idpath/idlevel/idcount；fields 为空时回退 cols）；
- `renderTree` 的节点标题改为 `主键: 第一个业务字段值`（第一个字段值为空/NULL 时
  仅显示主键）。

**验证**（逻辑模拟）：atree → `100: 根节点`、`4: ssd`（第一个业务字段 kkk）；
aaa → `7: 华东`（第一个业务字段 aaa）；第一个字段为 NULL 的节点只显示主键。
app.js 已同步部署目录并经 nginx 线上确认（app.js?v=104）。


---

## 管理台：新建表删字段、修改表外键显示、父子表基于已有表、生成预览刷新（2026-11-16）

**1) 新建表/修改表：字段行删除功能强化**
- 字段行删除按钮由"×"改为醒目的"删除"文字按钮（红色边框、悬停填充）；
- 修复删除按钮被挤出可视区域的问题（`.nt-field` 网格列 5→6 列，admin.css v5→v6）；
- 新增 `delFieldRow`：修改表模式下删除字段需确认（"保存后该列将从表中删除（含数据）"），
  并同步刷新外键行的字段下拉。

**2) 修改表：外键配置行正确显示**
- 根因：`showAlter` 先读外键（此时字段列表尚未填充，外键行的"外键字段"下拉无选项，
  数据库里的外键字段名设置不上），后填字段列表；
- 修复：调整为**先填充字段列表、再读取并渲染外键配置行**。

**3) 父子表模块支持基于已有表生成（不再现场建表）**
- 新建表弹窗的父子表模式：父表/子表均从**已建的表**中选择（子表下拉排除父表），
  并选择**子表中与父表主键对应的关联字段**（下拉为子表全部列，默认选中与父表主键
  同名的列；子表无同名列时可任选）；
- 生成的联动模块：函数名以 **父表_子表** 为前缀（如 `zz_fp_zz_fc_query/add/edit/merge/del`），
  与父表/子表各自单独生成的 CRUD **不重名**；`modules.js` 生成独立模块
  （`P('父表_子表',…)` + `SUBTABLECFG.push`，editApi 指向联动 `_edit`）；
- 不再生成 CREATE TABLE（表已存在）；子表业务列由后端按子表实际列自动派生
  （排除主键/关联字段/结构列）；子表的 isused/systemid 按实际结构条件化写入
  （`_child_sync` 同步改造）；
- 权限：菜单+操作节点照常生成（actionurl 以联动前缀命名，按 actionurl 判重）；
- 暂以 plpgsql 生成（plpython3u 模板未适配联动前缀）。

**4) 生成模板预览旧数据**
- 点"生成"时先按当前表结构刷新字段列表（父子表另刷新子表列/关联字段下拉），
  修复"表结构已改、生成预览仍是旧模板数据"的问题。

**验证**（端到端实测，测试表 zz_fp/zz_fc 测后已清理）：建表 → 生成（函数名
`zz_fp_zz_fc_*`、无 CREATE TABLE、无子表独立函数、SUBTABLECFG/editApi 正确）→
发布（函数创建、热重载成功）→ 联动新增（父行+2 子行同事务）与联动查询（返回
`zz_fcjson`）均 errorcode 0 → 全部测试痕迹清理干净（表/函数/权限/modules.js/lastgen）。

---

## 2026-11-17 父子表函数命名规范 + 修复 /api/gen 空响应

**函数命名规范（父子表联动模块）**：固定为 **`父表名_子表名_query/add/edit/merge/del`**
共 5 个，**不含 undel**（恢复由单表模块负责），与父/子表各自单独生成的 CRUD 不重名：

- `gen_sql`：existing 父子表只生成 5 个联动函数（不再生成 undel）；
- 权限节点同步只生成 5 操作（build_def 剔除 undel、load_def 校验放宽、
  modules.js 的 `undel:null`）；
- 文档更新：`README.md`、`docs/DEVELOPMENT.md`、`.claude/skills/ngxdb-development/SKILL.md`
  均补充"基于已有表的父子联动模块"命名与 5 操作说明。

**修复：管理台 /api/gen 空响应（服务器返回了不可解析的内容）**：

- 根因：`tools/gen_module.py` 的 `load_def` 对保存的 `_def.json` 做二次校验时，
  对 5 操作的 existing 父子表仍按旧 6 操作规则检查，并以 **`sys.exit()`** 报错——
  `SystemExit` 不被管理台 `do_POST` 的 `except Exception` 捕获，请求线程静默死亡，
  前端收到空响应（HTTP 000 / "Empty reply"）；
- 修复：`load_def` 校验按 `existing` 父子表放宽为 5 操作；所有校验失败由
  `sys.exit` 改为 **`raise ValueError`**（可被正常捕获返回 400）；命令行入口
  捕获 ValueError 保持友好报错。

**验证**：管理台重启后端到端实测——建表 → `/api/gen` HTTP 200，生成函数恰为
`zz_fp_zz_fc_add/del/edit/merge/query`（5 个、无 undel）、无 CREATE TABLE、
模块 id `zz_fp_zz_fc` + SUBTABLECFG + editApi 正确、权限段无 undel 节点；
测试表已清理，modules.js 无残留。

---

## 2026-11-17 发布前文档/代码一致性审查

- 全量核对 11 个文档与代码/实际行为的一致性，修正以下偏差：
  - 权限节点幂等语义：`ON CONFLICT DO NOTHING` → `ON CONFLICT (sysactionid) DO UPDATE`
    （README 12.4 示例 SQL 与 12.5 清单、DEVELOPMENT.md 3.5 流程表/清单、SKILL.md、
    reference.md 模板，与生成器自愈迁移行为统一）；
  - README 12.5 上线清单第 4 步：`systemctl restart nginx` → `/_reloadfunctions` 热重载
    （免重启，与其他文档一致）；
  - 函数/表数量：README 手工安装示例"81 函数 / 19 表"、INSTALL.md"19 表" → 统一为
    **82 函数 / 20 表**（与 extension/ngxdb_postgresql.sql 事实源一致）；
  - 字段类型数："25 种" → **白名单 28 种（含别名）**，补充 `character(n)`、
    `timestamp with time zone` 别名及 `[]` 数组后缀说明（多选外键使用）；
  - reference.md 示例清单：移除已删除的 `example_subtask.json` 引用，按实际文件更正
    （notice/task=单表、parent_child=父子表、region=树表）；
  - install.sh 步骤标签 `[4.6/7]` → `[4.6/6]`（分母统一）；
  - INSTALL.md 安装步骤描述补充"本地管理台部署与 systemd 开机自启注册"；
- `.gitignore` 加固：排除 `admin/data/`（运行数据）与 `admin/config.json`、
  `admin/config.local.json`（含数据库口令的本地配置），防止入库；
- 复核确认无问题的项：文档无测试表（aaa/atree/sys_dict）残留、extension SQL 事实源干净、
  项目 html/ 与部署目录逐字节一致、无 TODO/FIXME、CHANGELOG 无敏感信息、
  全部 Python/JS/bash 语法通过（js_check 的 3 处报警均为正则字面量误报，人工复核无误）。

---

## 2026-11-17 GitHub 公开发布前安全整改

- **移除代码中的硬编码数据库口令**：`tools/gen_module.py`、`admin/app.py`、
  `test/test_ngxdb_web.py` 不再内置 postgres 口令，一律改为环境变量
  （`PGHOST/PGUSER/PGPASSWORD/PGDATABASE`）或 `admin/config.local.json` 本地配置提供；
- **一键安装口令随机化**：`install.sh` 未显式指定 `PG_APP_PASSWORD` 时自动生成 20 位
  随机口令（写入 nginx.conf 并在安装完成输出中提示保存），不再使用固定默认值；
- **文档去敏感化**：README/INSTALL 手动安装示例中的真实口令全部替换为
  `<数据库强口令>` 占位符；示例路径 `/home/ad/...` 改为 `/path/to/nginx_db`；
- 本机运行配置外置到 `admin/config.local.json`（已被 .gitignore 排除），管理台功能不受影响。


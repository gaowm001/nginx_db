# NGXDB

基于 **nginx（自研 C 模块）+ PostgreSQL 18** 的轻量级 RBAC 权限管理控制台。前端零依赖（纯 HTML/CSS/JS），全部业务逻辑以存储过程/函数形式实现在 PostgreSQL 中；nginx 通过自研模块把 HTTP 请求直接映射为数据库函数调用，**无需 PHP/Node/Java 等中间层**。

## 一、项目介绍

### 1.1 它是什么
NGXDB 是一个"权限/系统管理"后台：管理系统、部门、角色、员工、功能权限、省市公共数据与操作日志，并提供基于令牌的登录鉴权，以及按人/按角色的功能权限控制。

### 1.2 架构
```
浏览器
  │  HTTP(:80)
  ▼
nginx
  ├── /           → 静态文件（html/ 复制而来：login.html, index.html, assets/*）
  └── /func/...   → ngx_http_postgres_module（自研 C 模块，libpq 直连）
                        ▼
                   PostgreSQL 18
                     · schema：sysinfo / gm / conn / crypto
                     · 19 张表、82 个函数（业务 70 + 内部 12：登录、鉴权、CRUD、AES 令牌、审计触发器等）
                     · 扩展：plpython3u、pgcrypto
```

要点：
- **前端零依赖**：`html/assets/{core.js,app.js,modules.js,suite.css}`，无框架、无构建，复制到 nginx 的 html 目录即可用。
- **无应用层**：所有接口都是 PostgreSQL 函数；nginx 模块负责"把 URL 变成函数调用"。
- **API 约定**：`POST /func/<schema>/<func>[_<op>]`（**斜杠分隔**，`application/x-www-form-urlencoded`）。存储过程参数名以 `p_` 开头，模块会自动去掉前缀再与请求参数匹配（例如函数 `sysinfo.login(p_loginname, p_pass, p_ip, p_system)` 对应请求参数 `loginname/pass/ip/system`）。注意：请勿使用 `schema.func` 点号形式——模块对无法解析的函数名会直接关闭连接（不返回 404 JSON）。
- **登录鉴权**：`/func/sysinfo/login` 返回 AES 加密的 `token` 与可用 `actions` 列表；后续请求携带 `token`，由 `gm.check_login` 校验。

### 1.3 目录结构
```
nginx_db/
├── extension/
│   └── ngxdb_postgresql.sql   # PostgreSQL 18 完整库脚本（schema/函数/表/数据/admin 账号）
├── html/                      # 前端（零依赖）
│   ├── login.html
│   ├── index.html
│   └── assets/{core.js, app.js, modules.js, suite.css}
├── postgres/                  # nginx 自研 PostgreSQL 模块
│   ├── ngx_http_postgres_module.c
│   └── config                 # 供 nginx --add-module 使用（链接 libpq）
├── oracle/                    # nginx 自研 Oracle 模块（可选，需 Oracle Instant Client）
│   ├── ngx_http_oracle_module.c
│   └── config
├── tools/                      # 开发与运维工具
│   ├── gen_module.py           # 新业务模块一键生成器（JSON → SQL/前端/文档）
│   ├── examples/               # 生成器示例定义（single/parent_child/tree）
│   ├── secure_deploy.sh        # 等保整改一键 root 部署（TLS/限流/ngxapp/备份 cron）
│   ├── nginx.conf.template     # 安全加固 nginx 配置模板
│   ├── backup.sh               # 每日 pg_dump 备份（保留 14 份）
│   └── ngxdb-backup.cron       # 备份 cron 模板
├── .claude/skills/             # 大模型代码生成 skill（开发规范+模板）
│   └── ngxdb-development/      # SKILL.md + reference.md
├── README.md                  # 本文件
└── install.sh                 # 一键安装脚本
```

### 1.4 功能模块（前端菜单）
| 分组 | 模块 | 说明 |
|---|---|---|
| 全系统管理(9001) | 权限管理 sysaction / 系统管理 systeminfo / 员工权限 operinfo / 系统日志 serverlog / 功能查询 actions / 出错代码 errorcode / 接口文档 help_query+help_edit | 跨系统的权限、系统、员工、日志、接口元数据（可在线维护注释） |
| 本系统管理(9002) | 部门 sysorg / 部门类型 orgtype / 角色 roleinfo / 员工部门 operinfoorg / 员工权限分配 operinfopermission | 当前系统的组织与授权 |
| 公共数据(9003) | 省 province / 市 city | 公共基础数据 |

### 1.5 本系统优点

1. **架构极简，部署即用**：整套系统只有 nginx + PostgreSQL 两个组件，无 Java/Python/Node
   应用层、无消息队列等中间件；一键安装脚本从编译到初始化数据一步到位，非常适合
   私有化、内网环境快速交付。
2. **数据库即后端，链路短**：HTTP 请求由 nginx postgres 模块直接调起数据库函数，
   没有应用层的序列化/转发开销；一个函数就是一段完整的业务逻辑，排查问题从接口
   到 SQL 一路到底。
3. **权限体系开箱即用**：基于 `sysaction` 权限树 + token 时效的统一鉴权
   （`gm.check_login`），菜单可见性与操作权限分层控制，支持多系统、多角色授权，
   admin 经根节点自动全权。
4. **接口契约统一**：所有接口都是 `POST /func/{schema}/{函数}`，返回统一的
   `{errorcode, message, info}` 结构；内置**接口文档页**自动展示全部函数的参数、
   返回值与说明，且注释支持在线编辑，前后端协作成本低。
5. **前端声明式开发**：`modules.js` 一行 `P()` 声明即可获得完整的增删改查页面
   （列表、模糊搜索、分页、弹窗表单、删除/恢复、导出、树形“资源管理器”两栏布局），
   大部分业务页面无需写 JS。
6. **新模块一键生成**：`tools/gen_module.py` 从一份 JSON 定义自动产出建表 SQL、
   六个标准函数、权限节点、前端配置乃至 README/TEST_PLAN 更新，覆盖单表、
   父子表、树形表三类常见形态，并支持事务试跑（`--apply-tx`）后再正式上线。
7. **可重复部署、可演进**：`extension/ngxdb_postgresql.sql` 是唯一事实来源，权限
   节点等写入均幂等（`ON CONFLICT (sysactionid) DO UPDATE`，重复发布自愈迁移），新环境按 dump 一步重建；人事、
   财务、ERP 等新业务按第十二章的模式即可持续叠加，互不干扰。
8. **安全细节到位**：业务函数统一 `SECURITY DEFINER` + 最小化 OWNER、SQL 全程
   参数化、错误码体系化（errorcode 表驱动文案）、关键操作留痕于系统日志，
   鉴权失败/越权访问在后端强制拦截。

## 二、环境要求
- 操作系统：Ubuntu 24.04 / Debian 12+（脚本用 apt；其他发行版请自行替换依赖安装命令）
- 编译依赖：build-essential、libpcre2-dev（或 libpcre3-dev）、zlib1g-dev、libssl-dev、libpq-dev
- 数据库：PostgreSQL 18，且具备 `plpython3u`（postgresql-plpython3-18）与 `pgcrypto` 扩展
- 可选：Oracle Instant Client（仅当需要 oracle 模块时）

## 三、安装

### 方式 A：一键安装（推荐）
```bash
sudo bash install.sh
```
脚本自动完成 6 步：**装依赖 → 编译安装 nginx(带 postgres 模块) → 初始化 PostgreSQL(建角色/建库、导入 SQL) → 生成 nginx.conf 并部署前端 → 启动 nginx → 用 admin/123456 验证登录**。

常用选项：
| 选项 | 说明 |
|---|---|
| `--with-oracle` | 同时编译 oracle 模块（需 Oracle Instant Client，并修正 `oracle/config` 的 CORE_INCS/CORE_LIBS） |
| `--skip-deps` / `--skip-nginx` / `--skip-db` / `--skip-html` / `--skip-verify` | 分步控制，可单独重跑某一步 |
| 环境变量 | `NGINX_PREFIX`(默认 /opt/nginx)、`LISTEN_PORT`(默认 80)、`PG_DBNAME`(默认 ngxdb)、`PG_APP_USER`(默认 gm)、`PG_APP_PASSWORD`、`NGINX_VERSION`(默认 1.30.6) 等 |

### 方式 B：手动分步安装
**（1）安装依赖**
```bash
sudo apt-get update
sudo apt-get install -y build-essential libpcre2-dev zlib1g-dev libssl-dev libpq-dev
sudo apt-get install -y postgresql postgresql-client postgresql-plpython3-18
```

**（2）编译安装 nginx（带本扩展）**
```bash
cd <nginx-1.30.6 源码目录>
./configure --prefix=/opt/nginx --with-http_ssl_module --with-threads \
            --add-module=/path/to/nginx_db/postgres
make -j"$(nproc)" && sudo make install
```
> 说明：`--add-module` 指向含 `config` 文件的模块目录；`postgres/config` 会自动链接 libpq（`-lpq`、`-I/usr/include/postgresql`）。

**（3）初始化数据库**
```bash
# SQL 中大量 OWNER TO gm / conn，两个角色必须先存在（口令请替换为强口令）
sudo -u postgres psql -c "CREATE ROLE gm   LOGIN PASSWORD '<数据库强口令>';"
sudo -u postgres psql -c "CREATE ROLE conn LOGIN PASSWORD '<数据库强口令>';"
sudo -u postgres createdb -O gm ngxdb
# 导入（含 schema / 82 函数 / 20 表 / 数据 / admin 账号）
sudo -u postgres psql -v ON_ERROR_STOP=1 -d ngxdb -f /path/to/nginx_db/extension/ngxdb_postgresql.sql
# nginx 模块会向 public.serverlog 写调用日志，需建表
sudo -u postgres psql -d ngxdb -c "CREATE TABLE IF NOT EXISTS public.serverlog(logid bigserial primary key, clientip inet, pckname varchar(255), funcname varchar(255), content text, logtime timestamp, serverip inet, params text, operatorid bigint, res text, head text);"
```

**（4）配置 nginx.conf（在 /opt/nginx/conf/nginx.conf 的 server 块中增加）**
```nginx
server {
    listen 80;
    server_name _;
    root   html;
    index  index.html index.htm;

    location / {
        try_files $uri $uri/ =404;
    }

    # NGXDB API：把 /func/<schema>/<func> 交给 postgres 模块执行
    location /func/ {
        rewrite ^/func/(.*)$ /$1 break;
        postgresconn "host=127.0.0.1 port=5432 dbname=ngxdb user=gm password='<数据库强口令>'";
    }
}
```

**（5）部署前端**
```bash
sudo cp -r /path/to/nginx_db/html/. /opt/nginx/html/
```

**（6）启动并验证**
```bash
/opt/nginx/sbin/nginx -t && sudo /opt/nginx/sbin/nginx
curl -s -X POST http://127.0.0.1/func/sysinfo/login \
  --data-urlencode loginname=admin --data-urlencode pass=123456 --data-urlencode system=100
# 返回含 "errorcode":0 与 token 即成功
```

## 四、使用说明

### 4.1 登录
- 访问：`http://<服务器IP>/login.html`
- 默认账号：`admin`  密码：`123456`
- 登录后进入控制台（`index.html`）：左侧为三大分组菜单，右侧为数据表格（查询/新增/修改/删除/恢复/详情）。

### 4.2 接口调用约定
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

### 4.3 权限体系
- `sysinfo.actions`：功能权限权威表（67 行：actionid / actionname / actionurl / params）。
- 登录时返回当前账号可用的 `actions`（actionid 列表），前端据此显隐菜单与操作按钮。
- 授权链：角色(roleinfo) → 角色权限(rolepermission)；员工(operinfo) → 员工权限(operpermission) 关联到 sysaction。

## 五、安全提示（重要）
1. **默认口令**：`admin/123456` 为演示口令，生产环境请立即修改（表 `sysinfo.operinfo`）。
2. **函数即接口**：nginx 模块会把所连库中非排除 schema 的函数都暴露为 URL。请置于内网 / 加反向代理鉴权 / 限制来源 IP，勿直接暴露公网。
3. **口令存放**：`nginx.conf` 的 `postgresconn` 内含数据库口令，注意文件权限（`chmod 600 /opt/nginx/conf/nginx.conf`）。
4. 数据库口令请勿使用任何示例占位值，务必设置为强口令（一键安装时由脚本随机生成）。
5. **子系统密钥（`systeminfo.prikey`）**：是签发/校验该子系统登录 token 的 AES 密钥（算法见 `systeminfo.algorithm`：1=AES-128，2=AES-256），泄露可伪造该系统登录态；修改后该系统全部在线 token 立即失效（用户需重新登录）。详见「十四、安全与性能验证」。

## 六、常见问题（FAQ）
- **登录 404 / 502**：确认 nginx 已 `--add-module=.../postgres`（`nginx -V`）；确认 nginx.conf 有 `/func/` location 且含 `rewrite ... break;`。
- **登录提示账号密码错误 / 无 token**：确认数据库 `ngxdb` 已导入、角色 `gm/conn` 存在、`plpython3u`/`pgcrypto` 扩展可用；查看 `/opt/nginx/logs/error.log`。
- **`libpq` 链接错误**：安装 `libpq-dev`。
- **`plpython3u` 缺失**：安装 `postgresql-plpython3-18`。
- **端口 80 被占用**：修改 `LISTEN_PORT` 或 nginx.conf 的 `listen`。
- **系统日志为空**：确认 `public.serverlog` 表存在（见"方式 B（3）"）。
- **Oracle 模块**：需安装 Oracle Instant Client 并提供 `oci.h`，且 `oracle/config` 的 CORE_INCS/CORE_LIBS 需指向你的 Oracle 库路径后再 `--with-oracle`。
- **登录后多个页面间歇性提示「子系统信息出错(100012)」或「非法登录(100005)」**：这是旧版 `postgres` 模块的缺陷——它在 POST 请求体尚未读全时就处理，导致 `token` 参数丢失。修复：重新编译并安装 nginx（`sudo bash rebuild_nginx.sh`，脚本已随附本目录）后即恢复正常。详见「八、修复记录」。

## 七、默认配置一览
| 项 | 默认值 |
|---|---|
| nginx 安装目录 | `/opt/nginx` |
| 监听端口 | `80` |
| 数据库名 | `ngxdb` |
| 数据库角色 | `gm` / `conn`（口令由安装脚本随机生成，或用环境变量 `PG_APP_PASSWORD` 指定） |
| 超级用户 | `postgres` |
| nginx 可执行文件 | `/opt/nginx/sbin/nginx` |
| 日志 | `/opt/nginx/logs/{access.log,error.log}` |

## 八、修复记录（2026-11-11）

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

## 九、功能更新：树状展示（2026-11-12）
- **范围**：表结构中带上级字段（`upid`）的模块——**权限管理(sysaction)**、**部门管理(sysorg)**。
- **后端**：`sysaction_query`/`sysorg_query` 返回值新增 `idpath`/`idlevel`，并改按 `idpath` 排序（父先子后的树序，树深度即 `idlevel-1`）。
- **前端**：`renderRows` 对树模块按 `idlevel` 层级缩进展示；有下级的行显示 **▼/▶**，点击可**折叠/展开**子树（按 idpath 前缀匹配隐藏后代）；行操作按钮改用**主键定位**（折叠后索引不错位）。
- **效果**：权限管理页呈现为「系统 → 系统管理 → 系统权限 → 新增/修改/…」的层级树；部门管理随部门数据增加自动成树。
- **涉及文件**：`extension/ngxdb_postgresql.sql`、`html/assets/modules.js`（`P()` 新增 `tree` 参数）、`html/assets/app.js`（`treeDepth`/`renderRows`）、`html/assets/suite.css`（树样式）。

### 9.1 树状模块升级为“资源管理器”两栏布局（2026-11-12）

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

### 9.2 数据库函数缺陷修复（2026-11-12，由测试发现）

测试套件（`test/`）运行暴露 4 个真实 SQL 缺陷，均已修复并同步到在用库（CREATE OR REPLACE）：

| 缺陷 | 涉及函数 | 后果 |
|---|---|---|
| `json_to_recordset(json, boolean)` 两参调用在 PG18 不存在 | operinfo_edit、operinfoorg_edit、operinfopermission_add/edit/merge、roleinfo_edit | 员工权限/角色编辑功能一执行即报错 |
| 权限/系统归属写入误用 `p_operatorid`（自动编号时为 null） | operinfo_add | 新增员工报 non-null 违约，功能不可用 |
| 权限 JSON 过滤条件 `AND` 恒假 | operinfo_add、operinfopermission_add | 权限永远写不进去 |
| 合并新建员工缺 sysoper 归属、delete 条件变量用错 | operinfoorg_merge | 新建员工无法登录/删除 |

修复后回归：**数据库层测试 141 通过 / 0 失败 / 4 已知问题**（已知问题为 dump 缺少 pkg_service、sms 模式及 funchtml 空表，见 `test/TEST_PLAN.md` 第 5 节）。

**2026-11-14 追加修复**：`operinfopermission_query` 分页分支引用不存在的别名 `toper` 导致接口报错，已修复；
员工/角色权限子表保存由“先删后插”改为“按子表主键增删改”（删除→更新→插入），子表主键保持稳定。

### 9.3 环境问题：systemd nginx.service 循环重启（重要）

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

## 十、功能更新：接口文档（2026-11-13）

- **原实现（已删除）**：`gm.gethelp()` 用 `sysinfo.funchtml`（htmlid=100）模板拼接
  `pg_proc/pg_description` 注释后直接返回**整页 HTML**。存在的问题：① 无鉴权，任何请求可调；
  ② 当前库 `funchtml` 为空表，实际返回 NULL；③ HTML 由数据库拼接，前端无法复用与检索。
- **新实现**：
  - 权限节点 **9113「接口文档」**，挂在权限树根「系统 100」之下（`idpath=100.9113`）。
    admin 因持有根节点 100 的授权自动可用（`gm.check_login` 按 idpath 前缀匹配），其他账号按需授权。
  - 新增 **`sysinfo.help_query(p_token)`**（原 gethelp 改名）：校验登录态与权限 9113 后，返回当前库全部业务函数的
    元数据 JSON（`schemaname/funcname/arguments/returntype/description`，当前共 118 条，随函数增加自动增长）。
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

## 十一、系统运行原理

### 11.1 总体链路

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
   └─ gm.returnjson(错误码, 数据)     ← 统一 JSON 包装：{errorcode, message, info}（错误码分段规范见 docs/DEVELOPMENT.md「错误码编码规范」）
```

### 11.2 一次典型请求的完整流程（以“查询员工列表”为例）

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

### 11.3 权限模型（新增模块必须理解）

- `sysinfo.sysaction` 是**权限树**：`systemid` 所属系统、`sysactionid` 节点号、
  `idpath` 全路径（如 `100.9113.224`）、`actionid` 操作点（为空表示菜单/目录节点，
  鉴权时取 `coalesce(actionid, sysactionid)`）。
- **菜单节点**（如 9113 接口文档）控制前端是否显示页面；**操作节点**（如 224 修改
  接口注释）控制某个写操作是否可调用。两者都挂在对应父节点之下。
- 员工授权写入 `sysinfo.operinfo`/关联表，登录时展开成 `actions` 数组下发前端；
  后端函数仍会再次校验（前端隐藏按钮只是体验优化，**不是安全边界**）。

### 11.4 前端机制

- `modules.js` 用 `P()` 声明每个模块：模块 ID、标题、菜单分组、权限点、接口 base、
  列定义、表单字段、操作权限、树形标志等。**大多数 CRUD 页面零定制**，由 `app.js`
  通用渲染（列表 + 搜索 + 分页 + 弹窗表单 + 删除/恢复 + 导出）。
- 少数特殊页面（如接口文档 `#/gethelp`）在 `app.js` 路由中特判渲染。
- 静态资源版本号在 `index.html`（当前 `v=10`）：改动 js/css 后必须 +1，否则浏览器
  缓存旧文件。

### 11.5 函数缓存与发布（重要）

nginx postgres 模块在 **worker 启动时缓存可调用的函数清单**。因此：
- 新增/改名/删除 PostgreSQL 函数后，必须 `sudo systemctl restart nginx`（或
  stop + 清 pid + start，见第九章 9.3），否则 HTTP 层报“函数不存在”。
- 只改函数体（`CREATE OR REPLACE` 同签名）不影响缓存，可不用重启。

## 十二、二次开发指南：新增业务系统（人事 / 财务 / ERP 等）

在本项目下做一套新业务（人事、财务、ERP、CRM…），本质上是重复四件事：
**建表 → 写函数 → 配权限 → 声明前端模块**。数据库即后端，不需要另起应用服务。
本章规范已固化为大模型代码生成 skill：`.claude/skills/ngxdb-development/`
（SKILL.md + reference.md），供 AI 编码助手在本仓库内自动遵循。

### 12.1 总体思路与目录规划

| 步骤 | 产物 | 说明 |
|---|---|---|
| 1. 规划 schema | `CREATE SCHEMA hr;` | 一个业务域一个 schema：hr（人事）、fin（财务）、erp… |
| 2. 建业务表 | `hr.emp` 等表 | `OWNER gm`，主键 `bigint`，建议带逻辑删除/审计字段 |
| 3. 写函数 | `hr.emp_query/add/edit/merge/del/undel` | 入口 `gm.check_login`，出口 `gm.returnjson` |
| 4. 配权限 | `sysinfo.sysaction` 节点 | 菜单节点 + 各操作节点，`INSERT ... ON CONFLICT (sysactionid) DO UPDATE`（重复执行自愈迁移） |
| 5. 声明前端 | `html/assets/modules.js` 的 `P()` | 大多数 CRUD 页面零 JS 定制 |
| 6. 部署验证 | psql 应用 + nginx restart + 冒烟 | 见 12.5 上线清单 |

### 12.2 方式一：gen_module.py 一键生成（推荐，覆盖大部分场景）

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
sudo systemctl restart nginx     # 新函数必须重启 nginx 才能经 HTTP 访问
```

生成器已处理的细节：函数注释（`COMMENT ON FUNCTION`，可在接口文档页在线维护）、
分页/模糊查询、逻辑删除与恢复、树形 `idpath/idlevel/idcount` 递归维护、
树表 `upname` 联查、循环父级与“有子级禁删”校验、树表建表自动内置 id=100 默认根节点
（幂等去重）、权限节点幂等插入。

**父子表（parent_child）与权限子表的“整表 JSON”语义**：以父表为主模块，子表不建独立接口——
父表 `_query` 每行聚合返回 `<子表>json`，父表 `_add/_edit/_merge` 接收 `p_<子表>json`
在**同一事务内按子表主键做增删改**（先按提交主键差集删除、再按主键更新、最后插入新行，
不整表先删后插，子表主键保持稳定，外部按子表主键的关联不会失效）；前端明细把子表整表
包装成 JSON 一次性提交。员工权限（`operinfopermission_edit`+`operpermissionjson`）、
角色权限（`roleinfo_edit`+`rolepermissionjson`）同此语义。系统内置操作员
（`operatorid in (10000,100000)`）禁止删除，相关 del 接口一律返回 **100036**。

**基于已有表的父子联动模块**：父表与子表均已存在时，管理台“生成模块”可直接选择
两表并指定子表的**关联字段**（默认取与父表主键同名的列）——生成独立联动模块：
函数名以 `父表_子表` 为前缀（`<父表>_<子表>_query/_add/_edit/_merge/_del`，共 5 个，
**不含 undel**，与父/子表各自单独生成的 CRUD 不重名）；`modules.js` 生成独立模块
（模块 id=`父表_子表`）并挂 `SUBTABLECFG`；权限节点照常生成（接口地址以联动前缀
命名，按 actionurl 判重）。不生成任何 CREATE TABLE；子表的 `isused/systemid`
按实际结构条件化写入。

### 12.3 方式二：手工开发（复杂业务，如 ERP 单据联动）

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
  v_check:=gm.check_login(p_token, 9201);                 -- ① 鉴权：权限点见 12.4
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

### 12.4 权限节点设置（以人事系统为例）

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

### 12.5 上线清单（每个新模块都走一遍）

1. SQL 已同步进 `extension/ngxdb_postgresql.sql`（唯一事实来源，手工开发也要回写 dump）。
2. `--apply`（或 psql 执行）到在用库；`sysaction` 插入用了 `ON CONFLICT (sysactionid) DO UPDATE`（幂等且自愈迁移），可重复执行。
3. `--deploy` 或手动拷贝 `html/` 到 `/opt/nginx/html/ngxdb`，`index.html` 版本号 +1。
4. 热加载函数清单：`curl "https://127.0.0.1/func/_reloadfunctions?token=<postgresreloadtoken>"`（新函数必须重载后才能经 HTTP 访问，免重启 nginx）。
5. 冒烟：无 token→100005；伪 token→100012；admin 带 token→0；写操作各自权限点生效。
6. 按 `test/TEST_PLAN.md` 模板补充并执行该模块的测试场景。

## 十三、开发注意事项（重要）

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
7. **nginx 函数缓存**：新增/改名/删除函数后必须重启 nginx（第九章 9.3 的 pid 坑）；
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

## 十四、安全与性能验证（实测，2026-11-13）

以下数据均在当前部署环境（VMware 虚拟机，nginx + PostgreSQL 18 同机，本机回环
`127.0.0.1`，nginx 单实例）实测取得，可复现。

### 14.1 安全验证（全部在在用系统上实测通过）

| # | 用例 | 输入 | 实际返回 | 结论 |
|---|---|---|---|---|
| 1 | 无 token 访问 | `POST /func/sysinfo/help_query`（不带 token） | `100005 非法登录` | 入口强制鉴权 ✓ |
| 2 | 伪造 token | `token=deadbeef00` | `100012 子系统信息出错` | 无法伪造会话 ✓ |
| 3 | 错误口令 | `loginname=admin, pass=wrongpass` | `100001 帐号密码错误` | 口令校验 ✓ |
| 4 | 登录名注入 | `loginname=admin'--` | `100001 帐号密码错误` | 参数化，注入无效 ✓ |
| 5 | 口令注入 | `pass=123456' OR '1'='1` | `100001 帐号密码错误` | 参数化，注入无效 ✓ |
| 6 | token 过期 | 事务内将 `tokentime` 置为过期后调用 | `100006 登录已失效`（回滚后恢复 0） | 时效控制 ✓ |
| 7 | 越权（无此权限点） | `gm.check_login(token, 99999)`（不存在的权限点） | `100008 无此权限` | 权限点强校验 ✓ |
| 8 | 未授权调用写接口 | 空 token / 失效会话调 `sysorg_add` 等写接口 | `100005 非法登录` | 写接口同样强制鉴权 ✓ |

说明：token 为 AES/PGP 加密串（`sysinfo.aes_decrypt`，密钥分两层：`actions(101).params`
与 `systeminfo.prikey`），并在 `check_login` 中二次解密比对 `operinfo.tokenkey/tokentime`；
所有业务函数入口强制调用，前端隐藏按钮仅是体验优化。

**子系统加密字段说明（`systeminfo.prikey` / `systeminfo.algorithm`）**：

- `systeminfo.prikey`：该子系统的 **AES 对称密钥**。登录签发 token 时，操作员信息
  （操作员编号、签发时间、有效期）先用该子系统 `prikey` 做**内层 AES 加密**，
  再用全局密钥（`actions(101).params`）做**外层加密**；`check_login` 校验时按相反
  顺序两层解密。因此**不同子系统的 token 互不相通**（密钥不同无法解密内层），
  无 `prikey` 也无法伪造或篡改令牌内容。
- `systeminfo.algorithm`：该子系统 token 使用的**加密算法标识**——`1 = AES-128`、
  `2 = AES-256`，签发与校验时按该值选择对应的加/解密处理。
- **注意**：修改某子系统的 `prikey` 或 `algorithm` 后，该系统**已签发的 token 将全部
  无法解密**（所有在线用户需重新登录）；请仅在密钥泄露等必要情况下轮换，并通知
  该系统用户重新登录。

### 14.2 性能验证（curl 实测，15 次取平均）

| 接口 | 场景 | 平均 | 最快 | 最慢 |
|---|---|---|---|---|
| `sysinfo/login` | 登录（PG 加密 + 写 token） | **12.4ms** | 9.2ms | 22.0ms |
| `sysinfo/help_query` | 全量函数元数据大 JSON（实测时 116 行） | **7.8ms** | 7.1ms | 9.2ms |
| `sysinfo/sysorg_query` | 部门树（递归 CTE） | **8.0ms** | 7.1ms | 9.4ms |
| `sysinfo/errorcode_query` | 普通分页列表 | **7.8ms** | 7.2ms | 9.3ms |

并发：50 个 `help_query` 请求、10 并发，总耗时 **0.40s（≈125 req/s）**——单 worker、
虚拟机、含完整鉴权与解密。单个请求端到端稳定在 **10ms 以内**（登录约 12ms），
其中鉴权（两层解密 + 多表校验）占比很小，绝大多数时间是 PG 执行业务 SQL 与
JSON 组装。对中后台管理系统而言，该延迟远低于人的感知阈值（约 100ms）。

### 14.3 与 Tomcat（Spring Boot 典型栈）的比较

| 维度 | 本系统（nginx + PG 函数） | Tomcat + Spring Boot（典型三层） |
|---|---|---|
| 请求链路 | HTTP → nginx postgres 模块 → PG 函数，**1 跳** | HTTP → Tomcat 容器线程 → Filter/Controller → Service → ORM → JDBC → PG，**5+ 跳** |
| 单请求开销 | 实测 ~8ms（含鉴权解密） | 同等 CRUD 通常在数十毫秒量级（含 ORM 映射、对象分配、序列化；因实现而异） |
| 运行时 | 无 JVM：nginx 常驻内存小、无预热 | JVM 需预热与较大堆内存（数百 MB 起） |
| 事务与一致性 | 一个函数 = 一个事务，plpgsql 异常自动回滚 | 依赖 `@Transactional` 正确传播，跨层易出错 |
| 权限控制 | 函数入口强制 `check_login`，漏写即不可用（默认安全） | 依赖拦截器/注解逐接口标注，存在漏配风险 |
| 新模块成本 | 6 个函数 + 1 行 `P()` 声明（或 gen_module.py 一键） | Controller/Service/Repository/DTO/VO 多层样板代码 |
| 生态与适用 | 以数据为中心的管理系统、中后台、内网私有化 | 复杂应用逻辑、消息/调度/文件/第三方集成等大型应用 |

结论：对于**以数据库为中心的管理系统**（人事、财务、ERP、各类台账），本架构用更短
的链路换取更低延迟与更小的部署面；Tomcat 生态在复杂应用集成上仍更强，二者定位不同。
需要说明：本机未安装 Java，未做同环境 Tomcat 实测，表中 Tomcat 侧为架构分析与
公开经验量级，供选型参考。

## 十五、等级保护 2.0 合规检查与整改（2026-11-13）

对照《网络安全等级保护基本要求》（GB/T 22239-2019）技术要求逐项自查，结果如下。

### 15.1 检查前已符合的控制点

| 等保要求 | 系统现状 |
|---|---|
| 登录失败处理（限制非法登录次数） | 10 分钟内失败>5 次锁定 10 分钟、3 小时内>10 次锁定 3 小时 |
| 会话超时自动退出 | token 有效期 180 分钟，滑动续期，过期返回 100006 |
| 会话并发控制 | 单人登录顶号（tokentype） |
| 访问控制（RBAC 到操作级） | sysaction 权限树 + operpermission/角色，函数入口强制 check_login |
| SQL 注入防护 | 全程参数化，注入实测无效 |
| 错误信息保护 | 统一错误码，不泄露堆栈/SQL |
| 身份标识唯一 | 账号唯一、token 与操作员绑定 |

### 15.2 本轮已整改项（代码/配置已落地并验证）

| # | 等保要求 | 整改前问题 | 整改措施 | 验证结果 |
|---|---|---|---|---|
| 1 | 身份鉴别-口令存储 | operinfo.pass **明文存储**（如 123456） | 全部改为 blowfish 单向哈希（crypto.crypt），登录/改密用 `crypt(输入,存量哈希)` 比对；存量明文已迁移；新增 BEFORE 触发器保证今后任何写入路径自动哈希 | 旧口令仍可登录（0）；库中 pass 全部为 `$2a$08$` 哈希 |
| 2 | 安全审计-日志含敏感信息 | loginlog **记录明文口令** | 删除 loginlog.pass 列，登录日志不再含口令 | loginlog 已无 pass 列 |
| 3 | 安全审计-操作审计 | serverlog 有表无数据，重要操作无审计 | 新增审计触发器：loginlog/operinfo/sysaction/roleinfo/systeminfo/operpermission/rolepermission 的增删改自动写入 serverlog（时间、客户端IP、对象、操作类型、数据快照、操作者）；check_login 成功后通过会话变量向审计提供操作者 | 实测登录/更新后 serverlog 出现 audit 记录 |
| 4 | 通信传输保密性 | 仅 HTTP 明文（口令、token 裸奔） | nginx 启用 TLSv1.2/1.3（自签证书，生产换 CA 证书），HTTP 301 跳转 HTTPS；增加 X-Frame-Options/nosniff/Referrer-Policy 头、server_tokens off | 见 tools/secure_deploy.sh 部署后验证 |
| 5 | 最小权限 | nginx 以 postgres **超级用户**连库，口令明文在 conf | 新建专用账号 `ngxapp`，仅授予函数 EXECUTE（无表读写权限）；密码随机并轮换 | 新库导入后 ngxapp 存在；直查业务表应被拒 |
| 6 | 身份鉴别-口令复杂度 | 可设 123456 等弱口令 | 改密/设密强制 ≥8 位且含字母+数字，拒绝常见弱口令；新增错误码 100035 | 弱口令返回 100035 |
| 7 | 数据备份 | 无自动备份 | tools/backup.sh：每日 pg_dump 自定义格式，本地保留 14 份，附恢复命令与异地同步示例；cron 每日 2 点执行 | 脚本试运行生成 dump 文件 |
| 8 | 边界-资源控制 | 无接口限流 | nginx limit_req（50r/s，burst 100）+ limit_conn，防暴力破解/资源耗尽 | 部署后超频请求被拒 |

### 15.3 部署侧整改执行方法

数据库层整改已随 dump 应用；nginx 层（TLS/限流/ngxapp 口令轮换/备份 cron）需要 root，
已封装为一键脚本：

```bash
sudo bash tools/secure_deploy.sh          # Linux
# Windows（管理员 PowerShell）：
# powershell -NoProfile -ExecutionPolicy Bypass -File tools\secure_deploy.ps1
# 执行内容：生成自签证书 → 轮换 ngxapp 口令 → 安装安全 nginx.conf（tools/nginx.conf.template）
#          → nginx -t 校验后重启（失败自动回滚） → 注册每日备份计划任务 → 试运行备份
# 验证：curl -k https://127.0.0.1/func/sysinfo/login ... （原 http 地址 301 跳转 https）
```

### 15.4 仍需管理/环境层面落实的事项

1. **双因素认证**：等保三级要求两种以上鉴别技术组合，当前仅口令；建议按定级增加
   OTP/证书/短信因子。
2. **三权分立**：系统管理员、审计管理员、安全管理员应分设账号（RBAC 已支持按
   权限点拆分，需建立管理制度并分别建号授权）。
3. **证书管理**：当前为自签证书，生产应更换为机构 CA 签发并纳入证书有效期监控。
4. **异地备份与恢复演练**：backup.sh 已附 rsync 异地同步示例，需落实目的端并定期
   演练 pg_restore 恢复。
5. **日志留存制度**：等保要求日志留存不少于 6 个月，需配套归档存储与定期核查流程。
6. **入侵检测与告警**：登录锁定、越权尝试等事件目前仅入库，建议接入监控告警
   （如日志平台关键字告警）。
7. **漏洞与补丁管理**：nginx/PostgreSQL 应纳入定期漏洞扫描与升级流程。
8. **物理环境、网络边界设备策略**等属于机房与网络层，需按部署环境另行评估。

### 15.5 局限性说明

- serverlog.clientip 记录的是 **PG 看到的连接地址**（本机部署时为 127.0.0.1），
  真实客户端 IP 以 nginx access log 为准；如需在审计行中记录真实 IP，可由
  nginx 将 `$remote_addr` 作为参数传入业务函数后再写入。
- 自签证书仅消除明文传输，浏览器会提示不受信任；生产请更换 CA 证书。
- 审计触发器覆盖核心权限/员工/系统表；新增业务表如需审计，可复用
  `gm.audit_trigger()` 为业务表创建同样的 AFTER 触发器。

## 十六、Windows 环境支持

项目脚本分为三类，Windows 下的可用性如下：

### 16.1 跨平台 Python 工具（直接可用）

| 脚本 | 说明 |
|---|---|
| `tools/gen_module.py` | 生成器本体跨平台；`--deploy` 的目标目录默认 Linux 为 `/opt/nginx/html/ngxdb`、Windows 为 `C:\nginx\html\ngxdb`，均可用环境变量 `NGXDB_DEPLOY_DIR` 覆盖。执行 `--apply/--apply-tx` 需 `psql` 在 PATH。 |
| `test/test_ngxdb_db.py` | 通过 psql 子进程执行，Windows 需 psql 在 PATH；口令用环境变量 `PGPASSWORD` 或 `--password` 传入。 |
| `test/test_ngxdb_web.py` | 默认目标 `https://127.0.0.1`（自动忽略自签证书）；也可 `--base http://...` 指向未启用 TLS 的实例。 |

### 16.2 PowerShell 工具（Windows 等价实现）

| Linux 版 | Windows 版 | 用途 |
|---|---|---|
| `tools/backup.sh`（cron 每日 2 点） | `tools/backup.ps1`（schtasks 每日 2 点，SYSTEM 运行） | pg_dump 备份，本地保留 14 份 |
| `tools/secure_deploy.sh` | `tools/secure_deploy.ps1` | 自签证书（openssl 或 .NET 自签）、轮换 ngxapp 口令、安装安全 nginx.conf、`nginx -t` 校验后重启、注册备份计划任务 |
| `test/run_all.sh` | `test/run_all.ps1` | 重建隔离测试库 → 数据库层测试 → 网页层测试 |

Windows 快速执行（管理员 PowerShell）：

```powershell
cd <项目目录>
powershell -NoProfile -ExecutionPolicy Bypass -File tools\secure_deploy.ps1 `
    -NginxDir C:\nginx -PgSuperPassword '<postgres口令>'
powershell -NoProfile -ExecutionPolicy Bypass -File test\run_all.ps1 -PgPassword '<postgres口令>'
```

要点：
- nginx.conf 由 `tools/nginx.conf.template` 生成，`__NGXAPP_PASS__` 替换为随机口令，
  证书路径改写为绝对路径；写入采用**无 BOM UTF-8**（nginx 不接受 BOM）。
- postgres 超级用户口令写入 `C:\nginx\conf\pg_super.pass`，ACL 限制为
  SYSTEM/Administrators，仅供计划任务中的备份脚本读取。
- 计划任务以 SYSTEM 身份每日 02:00 运行 `backup.ps1`。

### 16.3 Windows 限制与说明

1. `install.sh`（apt/systemd 编译安装）与 `rebuild_nginx.sh`（gcc 重新编译 nginx）
   为 **Linux 专用**；Windows 下请使用 nginx 官方 Windows 二进制，postgres 模块需
   自行用 MSVC 编译（或使用已含该模块的构建）。
2. nginx 官方对 Windows 的定位是开发/演示；生产环境仍建议 Linux 部署。
3. `sudo`/`systemctl`/`cron` 相关逻辑在 Windows 分别由管理员权限、`nginx -s stop`
   + 进程启动、`schtasks` 计划任务替代。
4. 自签证书在浏览器同样会提示不受信任，生产请更换 CA 证书。

## 十七、本地管理台（admin）

`admin/` 是一个**独立**的本地开发/运维工具（与 ngxdb 登录权限体系无关、免登录、仅限本机使用），
用于管理本地 PostgreSQL 表并把 **单表 / 父子表 / 树表** 一键生成为 ngxdb 业务模块：

1. **模式管理**：新建模式（schema，可附中文说明），创建时自动完成与 `sysinfo`/`gm` 一致的
   最小授权，确保发布的函数可被 nginx（conn 账号）直接调用。
2. **表管理**：新建/修改/删除本地 PostgreSQL 表。新建表**不强制** `isused/systemid` 结构列
   （弹窗内提供"＋ 启用列(isused)"、"＋ 系统列(systemid)"快捷按钮，由用户决定是否添加）；
   字段类型覆盖 PostgreSQL 常用类型（白名单 28 种，含别名写法，支持 `[]` 数组后缀如
   `integer[]`），`varchar/char/numeric` 的长度/精度可自定义；
   树表自动带树形结构列并**内置 id=100 的默认根节点**（幂等去重，重复执行不重复插入），
   父子表可一次建父表+子表；**修改表与新建表同表单**（预填表信息，
   按差异生成 ADD/ALTER TYPE/COMMENT/DROP 事务，结构与主键列受保护；删除列时自动清理
   该列的外键登记 fk_meta）；**删除表**连带清理生成的函数、权限节点（sysaction/actions）、
   前端模块配置（modules.js）与外键登记。
3. **模块生成**：选择表并指定表类型（single / parent_child / tree）→ 在**权限树**中选择挂载
   权限节点（树支持模糊搜索定位、节点增删改、同级上移/下移——搜索出多个结果时可在搜索
   结果间移动）→ 字段勾选 → 一键生成 plpgsql SQL + modules.js 前端
   配置与权限节点（操作权限**同步写入 actions 具体权限表**，按 actionurl 判重），附页面预览；
   操作权限 actionid 修改后其后的 ID 自动顺延 +1；可指定权限归属**系统**（systemid）。
   **重新生成时优先载入该表已发布的权限**——菜单/权限 ID 保持稳定，不重复生成权限节点；
   **父子表**生成时父表与子表均从**已建的表**中选择（子表关联字段下拉为子表全部列，
   默认选中与父表主键同名的列），生成**独立联动模块**：函数名为
   `父表名_子表名_query/add/edit/merge/del`（共 5 个，**无 undel**，与父/子表各自
   单独生成的 CRUD 不重名），模块 id 为 `父表_子表`，不生成建表语句。
4. **发布**：事务应用 SQL 到业务库（自动探测 nginx postgres 模块实际连接的库）→ 同步
   modules.js/index.html 到项目与部署目录 → 经 `/func/_reloadfunctions` 热重载 → 新模块即刻
   在 ngxdb 前端可用；**ngxdb 前端左侧菜单按 sysaction 权限树动态渲染**：目录/功能节点生成
   多级菜单，**指向具体函数的操作级节点（actionid 非空）表示到尾部、不生成菜单**——菜单到
   功能级为止（如 ERP→字典）；功能节点有对应模块页面的直接进入（子页面代码不变），暂无
   模块页面的显示空占位页；表格列头显示字段中文注释。

入口：`http://127.0.0.1:8765/`（后端仅监听本机，页面由后端直接服务，不经 nginx）。
一键安装脚本已集成（`--skip-admin` 可跳过）；手动部署：`bash admin/deploy.sh`。
安全边界：来源限本机、标识符/类型白名单、写路径白名单、SQL 事务回滚、发布前自动备份。
详见 `admin/README.md`。

## 十八、许可证

本项目以 [MIT License](LICENSE) 发布。

- `postgres/ngx_http_postgres_module.c` 为原创实现的 nginx PostgreSQL 网关模块，
  运行时依赖 libpq（PostgreSQL 客户端库，PostgreSQL License）；
- 各目录内保留的第三方版权与许可声明（如有）仍归其原作者所有。



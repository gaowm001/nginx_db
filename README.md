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
                     · 19 张表、77 个函数（登录、鉴权、CRUD、AES 令牌等）
                     · 扩展：plpython3u、pgcrypto
```

要点：
- **前端零依赖**：`html/assets/{core.js,app.js,modules.js,suite.css}`，无框架、无构建，复制到 nginx 的 html 目录即可用。
- **无应用层**：所有接口都是 PostgreSQL 函数；nginx 模块负责"把 URL 变成函数调用"。
- **API 约定**：`POST /func/<schema>.<func>[_<op>]`（`application/x-www-form-urlencoded`）。存储过程参数名以 `p_` 开头，模块会自动去掉前缀再与请求参数匹配（例如函数 `sysinfo.login(p_loginname, p_pass, p_ip, p_system)` 对应请求参数 `loginname/pass/ip/system`）。
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
├── README.md                  # 本文件
└── install.sh                 # 一键安装脚本
```

### 1.4 功能模块（前端菜单）
| 分组 | 模块 | 说明 |
|---|---|---|
| 全系统管理(9001) | 权限管理 sysaction / 系统管理 systeminfo / 员工权限 operinfo / 系统日志 serverlog / 功能查询 actions / 出错代码 errorcode / 接口文档 gethelp | 跨系统的权限、系统、员工、日志、接口元数据 |
| 本系统管理(9002) | 部门 sysorg / 部门类型 orgtype / 角色 roleinfo / 员工部门 operinfoorg / 员工权限分配 operinfopermission | 当前系统的组织与授权 |
| 公共数据(9003) | 省 province / 市 city | 公共基础数据 |

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
# SQL 中大量 OWNER TO gm / conn，两个角色必须先存在
sudo -u postgres psql -c "CREATE ROLE gm   LOGIN PASSWORD 'gmo@12345';"
sudo -u postgres psql -c "CREATE ROLE conn LOGIN PASSWORD 'gmo@12345';"
sudo -u postgres createdb -O gm ngxdb
# 导入（含 schema / 78 函数 / 19 表 / 数据 / admin 账号）
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

    # NGXDB API：把 /func/<schema>.<func> 交给 postgres 模块执行
    location /func/ {
        rewrite ^/func/(.*)$ /$1 break;
        postgresconn "host=127.0.0.1 port=5432 dbname=ngxdb user=gm password='gmo@12345'";
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
curl -s -X POST http://HOST/func/sysinfo/gethelp -d "token=<TOKEN>"
```

### 4.3 权限体系
- `sysinfo.actions`：功能权限权威表（67 行：actionid / actionname / actionurl / params）。
- 登录时返回当前账号可用的 `actions`（actionid 列表），前端据此显隐菜单与操作按钮。
- 授权链：角色(roleinfo) → 角色权限(rolepermission)；员工(operinfo) → 员工权限(operpermission) 关联到 sysaction。

## 五、安全提示（重要）
1. **默认口令**：`admin/123456` 为演示口令，生产环境请立即修改（表 `sysinfo.operinfo`）。
2. **函数即接口**：nginx 模块会把所连库中非排除 schema 的函数都暴露为 URL。请置于内网 / 加反向代理鉴权 / 限制来源 IP，勿直接暴露公网。
3. **口令存放**：`nginx.conf` 的 `postgresconn` 内含数据库口令，注意文件权限（`chmod 600 /opt/nginx/conf/nginx.conf`）。
4. 数据库口令建议不要沿用默认值 `gmo@12345`。

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
| 数据库角色 | `gm` / `conn`（默认密码 `gmo@12345`） |
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
  - 新增 **`sysinfo.gethelp(p_token)`**：校验登录态与权限 9113 后，返回当前库全部业务函数的
    元数据 JSON（`schemaname/funcname/arguments/returntype/description`，当前共 115 条）。
  - 前端「全系统管理」分组新增**接口文档**只读页面（`#/gethelp`）：表格展示 + 模糊搜索
    （模式/函数名/参数/返回类型/说明全列匹配）+ 分页 + 刷新。
- **涉及文件**：`extension/ngxdb_postgresql.sql`（删 `gm.gethelp`、增 `sysinfo.gethelp` 与
  权限节点 9113）、`html/assets/modules.js`（菜单项）、`html/assets/app.js`（renderGetHelp）、
  `html/index.html`（资源版本 v=9）。
- **部署注意**：nginx postgres 模块在 **worker 启动时缓存函数清单**，新增接口必须让 nginx
  重新加载后才可访问：
  ```bash
  sudo systemctl restart nginx
  # 若 pid 文件已被清空导致 ExecStop 停不掉手工实例（restart 失败），改用：
  # sudo systemctl stop nginx; sudo pkill -x nginx; sudo rm -f /opt/nginx/logs/nginx.pid; sudo systemctl start nginx
  ```

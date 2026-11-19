# NGXDB

![version](https://img.shields.io/badge/version-1.0.1-blue) ![platform](https://img.shields.io/badge/platform-Linux%20%7C%20Windows-lightgrey) ![db](https://img.shields.io/badge/PostgreSQL-18-blue) ![nginx](https://img.shields.io/badge/nginx-1.30.6-green) ![license](https://img.shields.io/badge/license-MIT-green)

基于 **nginx（自研 C 模块）+ PostgreSQL 18** 的轻量级 RBAC 权限管理控制台。前端零依赖（纯 HTML/CSS/JS），全部业务逻辑以存储过程/函数形式实现在 PostgreSQL 中；nginx 通过自研模块把 HTTP 请求直接映射为数据库函数调用，**无需 PHP/Node/Java 等中间层**。

一个"权限/系统管理"后台：管理系统、部门、角色、员工、功能权限、省市公共数据与操作日志，提供基于令牌的登录鉴权与按人/按角色的功能权限控制。

## ✨ 特性

- **无应用层架构**：HTTP 请求由 nginx 的 `ngx_http_postgres_module`（libpq 直连）直接调用 PostgreSQL 函数并返回 JSON，链路只有一跳
- **前端零依赖**：`html/assets/{core.js,app.js,modules.js,suite.css}`，无框架、无构建，复制即用
- **权限体系开箱即用**：`sysaction` 权限树 + token 时效统一鉴权（`gm.check_login`），菜单可见性与操作权限分层控制，支持多系统、多角色授权
- **接口契约统一**：全部接口为 `POST /func/{schema}/{函数}`，返回统一的 `{errorcode, message, info}`；内置接口文档页（可在线维护函数注释）
- **声明式前端**：`modules.js` 一行 `P()` 声明即得完整 CRUD 页面（列表、搜索、分页、弹窗表单、删除/恢复、导出、树形"资源管理器"两栏布局）
- **新模块一键生成**：`tools/gen_module.py` 从 JSON 定义自动产出建表 SQL、6 个标准函数、权限节点与前端配置，覆盖单表/父子表/树形三类
- **可重复部署**：`extension/ngxdb_postgresql.sql` 是唯一事实来源，权限节点等写入均幂等
- **安全细节**：函数统一 `SECURITY DEFINER`、SQL 全程参数化、口令 blowfish 哈希存储、审计触发器、错误码表驱动文案
- **跨平台**：Linux 一键安装（install.sh）+ Windows 一键安装（install.py，图形界面）
- **本地管理台 admin**：浏览器中直接管理本地表并一键生成业务模块（见 [admin/README.md](admin/README.md)）

## 🚀 快速开始

### Linux（Ubuntu 24.04 / Debian 12+）

```bash
git clone https://github.com/gaowm001/nginx_db.git
cd nginx_db
sudo bash install.sh          # 依赖 → 编译 nginx(含 postgres 模块) → 初始化 PG → 部署前端 → 启动 → 验证
```

安装完成后访问 `http://<服务器IP>/login.html`，默认账号 `admin / 123456`。

### Windows

下载仓库后，以管理员身份双击 **`安装.bat`**（或运行 `py -3 install.py`）进入图形安装向导：填端口/版本 → 点"开始安装"。安装完成后双击 **`启动.bat`** 日常启动，或用窗口底部的"打开系统入口 / 打开管理台"按钮。

> Windows 安装会自动下载 PostgreSQL 18 便携版与 nginx 1.30.6，检测并按需安装编译工具（MinGW-w64 / MSVC），现场编译含自研 postgres 模块的 nginx.exe。全过程日志写入 `install.log`。卸载：安装窗口的"卸载..."按钮按项勾选，或 `py -3 install.py --cli --uninstall-all` 全部卸载。

默认账号均为 **admin / 123456**（生产环境请立即修改，见[安全提示](#安全提示)）。

## 📖 文档

| 文档 | 内容 |
|---|---|
| [docs/INSTALL.md](docs/INSTALL.md) | 完整安装指南（Linux 三种方式 + Windows + 手动分步） |
| [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) | 二次开发指南、系统运行原理、错误码规范 |
| [docs/SECURITY.md](docs/SECURITY.md) | 安全设计与等保 2.0 合规整改 |
| [docs/PERFORMANCE.md](docs/PERFORMANCE.md) | 性能实测数据 |
| [docs/WINDOWS.md](docs/WINDOWS.md) | Windows 环境支持与限制 |
| [CHANGELOG.md](CHANGELOG.md) | 按日期的修复记录与功能更新 |
| [admin/README.md](admin/README.md) | 本地管理台使用说明 |
| [test/TEST_PLAN.md](test/TEST_PLAN.md) | 测试计划 |

## 📦 仓库结构

```
nginx_db/
├── install.sh                # Linux 一键安装（Ubuntu/Debian）
├── install.py                # Windows 一键安装/卸载（图形界面）
├── 安装.bat / 启动.bat        # Windows 双击安装 / 日常启动
├── rebuild_nginx.sh          # 修改模块源码后重新编译 nginx（Linux）
├── extension/
│   └── ngxdb_postgresql.sql  # PostgreSQL 18 完整库脚本（schema/函数/表/数据/admin 账号）
├── html/                     # 前端（零依赖）
│   ├── login.html / index.html
│   └── assets/{core.js, app.js, modules.js, suite.css}
├── postgres/                 # nginx 自研 PostgreSQL 模块（C，libpq）
│   ├── ngx_http_postgres_module.c
│   └── config                # 供 nginx --add-module 使用
├── oracle/                   # nginx 自研 Oracle 模块（可选，需 Oracle Instant Client）
├── admin/                    # 本地管理台（独立工具，免登录、仅限本机）
├── tools/                    # 开发与运维工具
│   ├── gen_module.py         # 新业务模块一键生成器（JSON → SQL/前端/文档）
│   ├── examples/             # 生成器示例定义
│   ├── secure_deploy.sh/.ps1 # 等保整改一键部署（TLS/限流/ngxapp/备份）
│   ├── backup.sh/.ps1        # 每日 pg_dump 备份（保留 14 份）
│   └── nginx.conf.template   # 安全加固 nginx 配置模板
├── test/                     # 测试（数据库层 + 网页层，run_all.sh/.ps1）
├── docs/                     # 详细文档（安装/开发/安全/性能/Windows）
├── .claude/skills/           # 大模型代码生成 skill（开发规范+模板）
├── CHANGELOG.md              # 修改记录
└── LICENSE                   # MIT
```

## 🧭 架构

```
浏览器
  │  HTTP(:80)
  ▼
nginx
  ├── /           → 静态文件（html/ 复制而来）
  └── /func/...   → ngx_http_postgres_module（自研 C 模块，libpq 直连）
                        ▼
                   PostgreSQL 18
                     · schema：sysinfo / gm / conn / crypto
                     · 20 张表、77 个函数（登录、鉴权、CRUD、AES 令牌、审计触发器等）
                     · 扩展：plpython3u、pgcrypto
```

## 🔌 API 约定

- 基础路径 `/func/`，调用方式 `POST`，`application/x-www-form-urlencoded`
- 命名：`/func/<schema>/<func>[_<op>]`，`op` ∈ `query/add/edit/merge/del/undel`
- 存储过程参数名以 `p_` 开头，模块自动去前缀与请求参数匹配（如 `sysinfo.login(p_loginname, p_pass, p_ip, p_system)` 对应请求参数 `loginname/pass/ip/system`）
- 返回 JSON：`errorcode=0` 成功；`rows`/`total` 为数据；错误码分段规范见 [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md)
- 登录：`POST /func/sysinfo/login` 返回 AES 加密 `token` 与可用 `actions`；后续请求携带 `token`，由 `gm.check_login` 校验

```bash
# 登录
curl -s -X POST http://HOST/func/sysinfo/login -d 'loginname=admin&pass=123456&system=100'
# 查询员工（token 用登录返回的 token）
curl -s -X POST http://HOST/func/sysinfo/operinfo_query -d "token=<TOKEN>&page=1&rows=20"
# 接口文档（权限 9113）
curl -s -X POST http://HOST/func/sysinfo/help_query -d "token=<TOKEN>"
```

> ⚠️ 请勿使用 `schema.func` 点号形式——模块对无法解析的函数名会直接关闭连接（不返回 404 JSON）。

## 🛠️ 二次开发

新增业务系统（人事/财务/ERP 等）= **建表 → 写函数 → 配权限 → 声明前端模块**，数据库即后端：

1. **一键生成（推荐）**：参照 `tools/examples/` 写 JSON 定义 → `python3 tools/gen_module.py --def my.json --apply-tx`（事务试跑）→ `--apply --deploy` 正式上线 → 重启 nginx
2. **手工开发**：复杂业务（多表事务/审批流）手工写函数，骨架照抄现有模块；规范见 [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md)
3. 开发规范已固化为大模型代码生成 skill：`.claude/skills/ngxdb-development/`

## 🔒 安全提示

1. **默认口令** `admin/123456` 仅供演示，生产环境请立即修改（表 `sysinfo.operinfo`）
2. **函数即接口**：nginx 模块会把所连库中非排除 schema 的函数都暴露为 URL，请置于内网 / 加反向代理鉴权 / 限制来源 IP，勿直接暴露公网
3. **口令存放**：`nginx.conf` 的 `postgresconn` 内含数据库口令，注意文件权限（`chmod 600`）
4. 数据库口令请勿使用示例占位值（一键安装时由脚本随机生成）
5. **子系统密钥**（`systeminfo.prikey`）是签发/校验 token 的 AES 密钥，泄露可伪造该系统登录态；轮换后该系统全部在线 token 立即失效

详见 [docs/SECURITY.md](docs/SECURITY.md) 与等保 2.0 合规整改记录。

## 📄 许可证

[MIT License](LICENSE)

- `postgres/ngx_http_postgres_module.c` 为原创实现的 nginx PostgreSQL 网关模块，运行时依赖 libpq（PostgreSQL License）
- 各目录内保留的第三方版权与许可声明（如有）仍归其原作者所有

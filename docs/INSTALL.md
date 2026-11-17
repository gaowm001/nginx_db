# NGXDB 安装与启动手册

本手册由 README.md 拆分而来（2026-11-13），内容随主 README 演进同步维护。

## 一、环境要求
- 操作系统：Ubuntu 24.04 / Debian 12+（脚本用 apt；其他发行版请自行替换依赖安装命令）
- 编译依赖：build-essential、libpcre2-dev（或 libpcre3-dev）、zlib1g-dev、libssl-dev、libpq-dev
- 数据库：PostgreSQL 18，且具备 `plpython3u`（postgresql-plpython3-18）与 `pgcrypto` 扩展
- 可选：Oracle Instant Client（仅当需要 oracle 模块时）

## 二、安装

### 方式 A：一键安装（推荐）
```bash
sudo bash install.sh
```
脚本自动完成 6 步：**装依赖 → 编译安装 nginx(带 postgres 模块) → 初始化 PostgreSQL(建角色/建库、导入 SQL) → 生成 nginx.conf 并部署前端（含本地管理台 admin 部署与 nginx systemd 开机自启注册）→ 启动 nginx → 用 admin/123456 验证登录**。

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
# （一键安装时口令由脚本随机生成并写入 nginx.conf；手动安装请自行使用强口令，
#   conn 由 tools/secure_deploy.sh 自动轮换为随机值——切勿带默认口令上线）
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
    location /func {
        rewrite ^/func/(.*)$ /$1 break;
        postgresconn "host=127.0.0.1 port=5432 dbname=ngxdb user=conn password='<数据库强口令>'";
        postgrespoolsize 16;                 # 每 worker 连接池空闲上限（0=禁用池）
        postgresreloadtoken __RELOAD_TOKEN__; # 热重载/连接池管理口令（生产请换随机值）
    }
}
```

> 连接账号使用最小权限角色 `conn`（仅函数 EXECUTE + 日志 INSERT，无业务表读写）；
> `postgresreloadtoken` 用于 `/_reloadfunctions`（函数清单热重载）与 `/_poolctl`
> （连接池热管理）的管理鉴权，未配置时仅允许 127.0.0.1 触发。
> 当前版本默认**不配置 token**（保持仅本机调用，方便试用）；需要远程管理时再配置随机 token。

**（5）部署前端**
```bash
sudo cp -r /path/to/nginx_db/html/. /opt/nginx/html/
# 也可部署到 /opt/nginx/html/ngxdb（URL 前缀 /ngxdb/，与本项目在线环境一致）
```

**（6）启动并验证**
```bash
/opt/nginx/sbin/nginx -t && sudo systemctl start nginx
curl -s -X POST http://127.0.0.1/func/sysinfo/login \
  --data-urlencode loginname=admin --data-urlencode pass=123456 --data-urlencode system=100
# 返回含 "errorcode":0 与 token 即成功
```

## 三、常见问题（安装与启动排障）
- **登录 404 / 502**：确认 nginx 已 `--add-module=.../postgres`（`nginx -V`）；确认 nginx.conf 有 `/func/` location 且含 `rewrite ... break;`。
- **登录提示账号密码错误 / 无 token**：确认数据库 `ngxdb` 已导入、角色 `gm/conn` 存在、`plpython3u`/`pgcrypto` 扩展可用；查看 `/opt/nginx/logs/error.log`。
- **`libpq` 链接错误**：安装 `libpq-dev`。
- **`plpython3u` 缺失**：安装 `postgresql-plpython3-18`。
- **端口 80 被占用**：修改 `LISTEN_PORT` 或 nginx.conf 的 `listen`。
- **系统日志为空**：确认 `public.serverlog` 表存在且 `conn` 有 INSERT 权限（见"方式 B（3）"）。
- **新增/修改函数后页面不生效**：执行热重载 `curl "https://127.0.0.1/func/_reloadfunctions?token=<postgresreloadtoken>"`（免重启）。
- **连接池状态/调优**：`curl "https://127.0.0.1/func/_poolctl?token=<postgresreloadtoken>"` 查看各入口池状态与命中统计。
- **Oracle 模块**：需安装 Oracle Instant Client 并提供 `oci.h`，且 `oracle/config` 的 CORE_INCS/CORE_LIBS 需指向你的 Oracle 库路径后再 `--with-oracle`。
- **登录后多个页面间歇性提示「子系统信息出错(100012)」或「非法登录(100005)」**：这是旧版 `postgres` 模块的缺陷——它在 POST 请求体尚未读全时就处理，导致 `token` 参数丢失。修复：重新编译并安装 nginx（`sudo bash rebuild_nginx.sh`，脚本已随附本目录）后即恢复正常。详见 CHANGELOG.md「一、修复记录」。

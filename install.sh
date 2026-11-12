#!/usr/bin/env bash
# ============================================================================
#  NGXDB 一键安装脚本 (Ubuntu/Debian)
#
#  流程：安装依赖 -> 编译安装 nginx(含 postgres 扩展) -> 初始化 PostgreSQL 18
#        -> 导入 extension/ngxdb_postgresql.sql -> 生成 nginx.conf -> 部署 html
#        -> 注册 systemd 服务(开机自启) -> 启动 nginx -> 验证登录(默认 admin/123456)
#
#  用法：  sudo bash install.sh [选项]
#
#  选项：
#    --with-oracle   同时编译 oracle 模块(需先装 Oracle Instant Client 并修正 oracle/config)
#    --skip-deps     跳过系统依赖安装
#    --skip-nginx    跳过 nginx 编译安装
#    --skip-db       跳过 PostgreSQL 初始化/导入
#    --skip-html     跳过前端 html 部署
#    --skip-verify   跳过登录验证
#    -h, --help      显示本帮助
#
#  可用环境变量覆盖默认值：
#    NGINX_VERSION NGINX_PREFIX LISTEN_PORT PG_MAJOR PG_HOST PG_PORT PG_DBNAME
#    PG_SUPERUSER PG_APP_USER PG_APP_PASSWORD BUILD_DIR
# ============================================================================
set -Eeuo pipefail

# ----------------------------- 默认参数 -----------------------------
NGINX_VERSION="${NGINX_VERSION:-1.30.6}"
NGINX_PREFIX="${NGINX_PREFIX:-/opt/nginx}"
LISTEN_PORT="${LISTEN_PORT:-80}"
PG_MAJOR="${PG_MAJOR:-18}"
PG_HOST="${PG_HOST:-127.0.0.1}"
PG_PORT="${PG_PORT:-5432}"
PG_DBNAME="${PG_DBNAME:-ngxdb}"
PG_SUPERUSER="${PG_SUPERUSER:-postgres}"
PG_APP_USER="${PG_APP_USER:-gm}"
PG_APP_PASSWORD="${PG_APP_PASSWORD:-gmo@12345}"
WITH_ORACLE="${WITH_ORACLE:-0}"
SKIP_DEPS=0; SKIP_NGINX=0; SKIP_DB=0; SKIP_HTML=0; SKIP_VERIFY=0
JOBS="$(nproc 2>/dev/null || echo 2)"

# ----------------------------- 路径 -----------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR"
SQL_FILE="$PROJECT_DIR/extension/ngxdb_postgresql.sql"
HTML_DIR="$PROJECT_DIR/html"
POSTGRES_MODULE_DIR="$PROJECT_DIR/postgres"
ORACLE_MODULE_DIR="$PROJECT_DIR/oracle"
BUILD_DIR="${BUILD_DIR:-/tmp/ngxdb-build}"

# ----------------------------- 日志 -----------------------------
if [ -t 1 ]; then C_R=$'\033[31m'; C_G=$'\033[32m'; C_Y=$'\033[33m'; C_B=$'\033[34m'; C_0=$'\033[0m'; else C_R=""; C_G=""; C_Y=""; C_B=""; C_0=""; fi
log(){  echo "${C_B}[NGXDB]${C_0} $*"; }
ok(){   echo "${C_G}[  OK ]${C_0} $*"; }
warn(){ echo "${C_Y}[ WARN]${C_0} $*"; }
err(){  echo "${C_R}[ERROR]${C_0} $*" >&2; }
die(){  err "$*"; exit 1; }
trap 'err "脚本执行失败(第 $LINENO 行: $BASH_COMMAND)"' ERR

# ----------------------------- 选项解析 -----------------------------
for a in "$@"; do
  case "$a" in
    --with-oracle) WITH_ORACLE=1 ;;
    --skip-deps) SKIP_DEPS=1 ;;
    --skip-nginx) SKIP_NGINX=1 ;;
    --skip-db) SKIP_DB=1 ;;
    --skip-html) SKIP_HTML=1 ;;
    --skip-verify) SKIP_VERIFY=1 ;;
    -h|--help) sed -n '2,23p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) warn "忽略未知参数: $a" ;;
  esac
done

# ----------------------------- 前置检查 -----------------------------
command -v apt-get >/dev/null 2>&1 || die "未检测到 apt-get（本脚本面向 Ubuntu/Debian）"
[ -f "$SQL_FILE" ]                 || die "找不到 SQL 文件: $SQL_FILE"
[ -d "$HTML_DIR" ]                || die "找不到前端目录: $HTML_DIR"
[ -f "$POSTGRES_MODULE_DIR/config" ] || die "找不到 postgres 模块 config: $POSTGRES_MODULE_DIR/config"

SUDO=""
if [ "$(id -u)" -ne 0 ]; then
  if command -v sudo >/dev/null 2>&1 && sudo -n true 2>/dev/null; then
    log "非 root 运行，检测到可用 sudo"
    SUDO="sudo"
  else
    die "请以 root 运行（sudo bash install.sh），或确保 sudo 可免密执行"
  fi
fi

# 以 PostgreSQL 超级用户执行 SQL（peer 认证，无需密码）
run_pg() {
  local q="$1" tmp out rc=0
  tmp="$(mktemp)"; printf '%s\n' "$q" > "$tmp"; chmod 644 "$tmp"
  set +e
  if [ "$(id -u)" -eq 0 ]; then
    out=$(su -s /bin/bash "$PG_SUPERUSER" -c "psql -v ON_ERROR_STOP=1 -tA -f '$tmp'" 2>&1); rc=$?
  else
    out=$($SUDO -u "$PG_SUPERUSER" psql -v ON_ERROR_STOP=1 -tA -f "$tmp" 2>&1); rc=$?
  fi
  set -e
  rm -f "$tmp"
  if [ $rc -ne 0 ]; then printf '%s\n' "$out" >&2; else printf '%s\n' "$out"; fi
  return $rc
}
run_pg_file() {
  local f="$1" db="$2"
  if [ "$(id -u)" -eq 0 ]; then
    su -s /bin/bash "$PG_SUPERUSER" -c "psql -v ON_ERROR_STOP=1 -d '$db' -f '$f'"
  else
    $SUDO -u "$PG_SUPERUSER" psql -v ON_ERROR_STOP=1 -d "$db" -f "$f"
  fi
}

# ----------------------------- 步骤 1：依赖 -----------------------------
step_deps() {
  if [ "$SKIP_DEPS" = "1" ]; then log "跳过依赖安装(--skip-deps)"; return 0; fi
  log "==== [1/6] 安装系统依赖 ===="
  $SUDO apt-get update -y
  $SUDO apt-get install -y \
      build-essential libpcre2-dev zlib1g-dev libssl-dev libpq-dev \
      postgresql postgresql-client
  # plpython3u 扩展(SQL 中用到)；pgcrypto 一般随 postgresql 主包
  $SUDO apt-get install -y "postgresql-plpython3-$PG_MAJOR"
  $SUDO apt-get install -y postgresql-contrib  || warn "postgresql-contrib 不可用(通常无影响)"
  $SUDO apt-get install -y libpcre3-dev        || warn "libpcre3-dev 不可用(将使用 libpcre2)"
  local ext_dir="/usr/share/postgresql/$PG_MAJOR/extension"
  [ -f "$ext_dir/pgcrypto.control"   ] || die "缺少 pgcrypto 扩展($ext_dir/pgcrypto.control 不存在)"
  [ -f "$ext_dir/plpython3u.control" ] || die "缺少 plpython3u 扩展(请安装 postgresql-plpython3-$PG_MAJOR)"
  ok "依赖安装完成"
}

# ----------------------------- 步骤 2：数据库 -----------------------------
step_db() {
  if [ "$SKIP_DB" = "1" ]; then log "跳过 PostgreSQL 初始化(--skip-db)"; return 0; fi
  log "==== [2/6] 初始化 PostgreSQL $PG_MAJOR ===="
  # 确保服务运行
  $SUDO service postgresql start >/dev/null 2>&1 \
     || $SUDO pg_ctlcluster "$PG_MAJOR" main start >/dev/null 2>&1 || true

  # 1) 角色(必须存在：SQL 中大量 OWNER TO gm / conn)
  run_pg "CREATE ROLE $PG_APP_USER LOGIN PASSWORD '$PG_APP_PASSWORD';" || warn "角色 $PG_APP_USER 已存在(忽略)"
  run_pg "CREATE ROLE conn LOGIN PASSWORD '$PG_APP_PASSWORD';"         || warn "角色 conn 已存在(忽略)"

  # 2) 数据库
  local exists
  exists="$(run_pg "SELECT 1 FROM pg_database WHERE datname='$PG_DBNAME'" || true)"
  if [ "$exists" = "1" ]; then
    warn "数据库 $PG_DBNAME 已存在，将直接导入(如需重建请先 DROP DATABASE $PG_DBNAME)"
  else
    run_pg "CREATE DATABASE $PG_DBNAME OWNER $PG_APP_USER;"
  fi

  # 3) 导入 SQL(含 schema/77 函数/19 表/数据/admin 账号)。复制到 /tmp 以保证 postgres 可读
  local sql_tmp; sql_tmp="$(mktemp /tmp/ngxdb_import.XXXXXX.sql)"
  cp "$SQL_FILE" "$sql_tmp"; chmod 644 "$sql_tmp"
  run_pg_file "$sql_tmp" "$PG_DBNAME"
  rm -f "$sql_tmp"
  ok "SQL 导入完成: $SQL_FILE -> 数据库 $PG_DBNAME"

  # 4) nginx 模块会把每次调用写入 public.serverlog，确保该表存在
  run_pg "CREATE TABLE IF NOT EXISTS public.serverlog (
             logid bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
             clientip inet, pckname varchar(255), funcname varchar(255),
             content text, logtime timestamp without time zone, serverip inet,
             params text, operatorid bigint, res text, head text);"
  run_pg "GRANT ALL ON public.serverlog TO $PG_APP_USER, conn;" || true
  run_pg "GRANT USAGE ON SCHEMA public TO $PG_APP_USER, conn;"  || true
  ok "PostgreSQL 初始化完成"
}

# ----------------------------- 步骤 3：编译 nginx -----------------------------
step_nginx() {
  if [ "$SKIP_NGINX" = "1" ]; then log "跳过 nginx 编译安装(--skip-nginx)"; return 0; fi
  log "==== [3/6] 编译安装 nginx $NGINX_VERSION ===="
  # 定位 nginx 源码：优先向上查找；否则下载
  local src="" d="$PROJECT_DIR"
  while [ "$d" != "/" ]; do
    if [ -f "$d/configure" ] && [ -f "$d/src/core/nginx.c" ]; then src="$d"; break; fi
    d="$(dirname "$d")"
  done
  if [ -n "$src" ]; then
    log "复用现有 nginx 源码: $src"
  else
    log "下载 nginx $NGINX_VERSION 源码..."
    mkdir -p "$BUILD_DIR"
    ( cd "$BUILD_DIR" \
        && { wget -q "https://nginx.org/download/nginx-$NGINX_VERSION.tar.gz" \
             || curl -fsSL -o "nginx-$NGINX_VERSION.tar.gz" "https://nginx.org/download/nginx-$NGINX_VERSION.tar.gz"; } \
        && tar -xzf "nginx-$NGINX_VERSION.tar.gz" )
    src="$BUILD_DIR/nginx-$NGINX_VERSION"
  fi
  [ -d "$src" ] || die "nginx 源码目录不存在: $src"
  cd "$src"
  rm -rf objs; rm -f Makefile
  local addons=(--add-module="$POSTGRES_MODULE_DIR")
  if [ "$WITH_ORACLE" = "1" ]; then
    [ -f "$ORACLE_MODULE_DIR/config" ] || die "oracle 模块目录缺失: $ORACLE_MODULE_DIR"
    addons+=(--add-module="$ORACLE_MODULE_DIR")
    log "已启用 oracle 模块(需已安装 Oracle Instant Client 并修正 oracle/config)"
  fi
  log "configure --prefix=$NGINX_PREFIX ${addons[*]}"
  ./configure --prefix="$NGINX_PREFIX" --with-http_ssl_module --with-threads "${addons[@]}"
  make -j"$JOBS"
  $SUDO make install
  ok "nginx 已安装到: $NGINX_PREFIX"
}

# ----------------------------- 步骤 4：nginx.conf + 前端 -----------------------------
step_conf_html() {
  log "==== [4/6] 生成 nginx.conf + 部署前端 ===="
  local conf_tmp; conf_tmp="$(mktemp /tmp/ngxdb_nginx.conf.XXXXXX)"
  cat > "$conf_tmp" <<'NGXCONF'
worker_processes  auto;

events {
    worker_connections  4096;
}

http {
    include       mime.types;
    default_type  application/octet-stream;
    sendfile      on;
    keepalive_timeout  65;

    server {
        listen       __LISTEN_PORT__;
        server_name  _;

        root   html;
        index  index.html index.htm;

        # ---- 前端静态文件 ----
        location / {
            try_files  $uri $uri/ =404;
        }

        # ---- NGXDB API ----
        # 前端约定调用 /func/<schema>.<func>[_<op>]；这里去掉 /func/ 前缀，
        # 交由 postgres 模块执行对应的存储过程(函数)，例如 /func/sysinfo/login。
        location /func/ {
            rewrite ^/func/(.*)$ /$1 break;
            postgresconn "host=__PG_HOST__ port=__PG_PORT__ dbname=__PG_DBNAME__ user=__PG_APP_USER__ password='__PG_APP_PASSWORD__'";
        }
    }
}
NGXCONF
  sed -i \
    -e "s|__LISTEN_PORT__|$LISTEN_PORT|g" \
    -e "s|__PG_HOST__|$PG_HOST|g" \
    -e "s|__PG_PORT__|$PG_PORT|g" \
    -e "s|__PG_DBNAME__|$PG_DBNAME|g" \
    -e "s|__PG_APP_USER__|$PG_APP_USER|g" \
    -e "s|__PG_APP_PASSWORD__|$PG_APP_PASSWORD|g" \
    "$conf_tmp"

  $SUDO mkdir -p "$NGINX_PREFIX/conf"
  if [ -f "$NGINX_PREFIX/conf/nginx.conf" ]; then
    $SUDO cp -a "$NGINX_PREFIX/conf/nginx.conf" "$NGINX_PREFIX/conf/nginx.conf.bak.$(date +%Y%m%d%H%M%S)"
  fi
  $SUDO cp "$conf_tmp" "$NGINX_PREFIX/conf/nginx.conf"
  rm -f "$conf_tmp"
  ok "nginx.conf 已生成: $NGINX_PREFIX/conf/nginx.conf"

  if [ "$SKIP_HTML" = "1" ]; then log "跳过前端部署(--skip-html)"; return 0; fi
  $SUDO mkdir -p "$NGINX_PREFIX/html"
  $SUDO cp -r "$HTML_DIR/." "$NGINX_PREFIX/html/"
  ok "前端已部署到: $NGINX_PREFIX/html"
}

# ----------------------------- 步骤 4.5：systemd 服务(开机自启) -----------------------------
use_systemd() { command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ]; }
step_service() {
  log "==== [4.5/6] 注册 nginx systemd 服务(开机自启) ===="
  if ! use_systemd; then
    warn "未检测到 systemd(容器/无 init 环境)，跳过服务注册，nginx 将以进程方式启动"
    return 0
  fi
  local bin="$NGINX_PREFIX/sbin/nginx"
  $SUDO tee /etc/systemd/system/nginx.service >/dev/null <<EOF
[Unit]
Description=NGINX HTTP server for NGXDB (installed by install.sh)
After=network.target

[Service]
Type=forking
PIDFile=$NGINX_PREFIX/logs/nginx.pid
ExecStartPre=$bin -t
ExecStart=$bin
ExecReload=$bin -s reload
ExecStop=-$bin -s stop
Restart=on-failure
RestartSec=5s
TimeoutStopSec=5s
KillSignal=SIGQUIT
PrivateTmp=true
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
EOF
  $SUDO systemctl daemon-reload
  $SUDO systemctl enable nginx >/dev/null 2>&1
  ok "已写入 /etc/systemd/system/nginx.service 并设为开机自启"
}

# ----------------------------- 步骤 5+6：启动 + 验证 -----------------------------
step_start_verify() {
  log "==== [5/6] 启动 nginx ===="
  local bin=""
  for b in "$NGINX_PREFIX/sbin/nginx" "$NGINX_PREFIX/nginx.exe" "$NGINX_PREFIX/nginx"; do
    if [ -x "$b" ]; then bin="$b"; break; fi
  done
  [ -n "$bin" ] || die "未找到 nginx 可执行文件(检查 $NGINX_PREFIX/sbin/nginx)"
  if use_systemd && [ -f /etc/systemd/system/nginx.service ]; then
    if [ -f "$NGINX_PREFIX/logs/nginx.pid" ]; then
      # restart 的 ExecStop(nginx -s stop)会停掉 pid 文件指向的旧实例(含手工启动的)，
      # 随后 ExecStart 以 systemd 托管方式重新拉起，避免双实例争抢端口。
      $SUDO systemctl restart nginx && ok "nginx 已由 systemd 托管重启"
    else
      $SUDO systemctl start nginx && ok "nginx 已由 systemd 启动"
    fi
    $SUDO systemctl enable nginx >/dev/null 2>&1
    [ "$($SUDO systemctl is-active nginx)" = "active" ] || die "systemd 启动 nginx 失败，查看: journalctl -u nginx --no-pager | tail -20"
  else
    $SUDO "$bin" -t
    if [ -f "$NGINX_PREFIX/logs/nginx.pid" ] && $SUDO "$bin" -s reload 2>/dev/null; then
      ok "nginx 已 reload"
    else
      $SUDO "$bin"
      ok "nginx 已启动(未注册 systemd 服务)"
    fi
  fi

  if [ "$SKIP_VERIFY" = "1" ]; then log "跳过登录验证(--skip-verify)"; return 0; fi
  log "==== [6/6] 验证登录 ===="
  sleep 1
  local code resp
  code="$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$LISTEN_PORT/login.html" || echo 000)"
  log "GET  /login.html -> HTTP $code"
  resp="$(curl -s -X POST "http://127.0.0.1:$LISTEN_PORT/func/sysinfo/login" \
        --data-urlencode "loginname=admin" \
        --data-urlencode "pass=123456" \
        --data-urlencode "system=100" || true)"
  log "POST /func/sysinfo/login -> ${resp:0:300}"
  if printf '%s' "$resp" | grep -q '"errorcode":0' && printf '%s' "$resp" | grep -q 'token'; then
    ok "登录验证成功！"
  else
    warn "登录验证未通过。排查：nginx -V 是否含 --add-module=.../postgres；"
    warn "PostgreSQL 是否运行；数据库 $PG_DBNAME 是否导入成功；nginx.conf 的 /func/ location 是否正确。"
    warn "错误日志：$NGINX_PREFIX/logs/error.log"
    return 1
  fi

  echo
  ok "================ NGXDB 安装完成 ================"
  ok "访问地址：http://<本机IP>:$LISTEN_PORT/login.html"
  ok "默认账号：admin     密码：123456     (生产环境请立即修改！)"
  ok "数据库：$PG_DBNAME (角色 $PG_APP_USER / conn)"
  if use_systemd && [ -f /etc/systemd/system/nginx.service ]; then
    ok "服务：nginx 已注册 systemd(开机自启)。管理命令：systemctl status|restart|stop nginx"
  fi
}

# ----------------------------- 主流程 -----------------------------
step_deps
step_db
step_nginx
step_conf_html
step_service
step_start_verify

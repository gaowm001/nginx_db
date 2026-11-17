#!/usr/bin/env bash
# ============================================================================
#  NGXDB · 重新编译并安装 nginx（含 postgres 模块“请求体完整读取”修复）
#
#  背景：原 ngx_http_postgres_module 作为 content handler 直接从
#        req->connection->buffer 读取 POST body，未等待请求体到达，导致
#        间歇性丢参(token) → 100005 非法登录 / 100012 子系统信息出错。
#        本脚本编译已修复的 postgres 模块并替换 /opt/nginx 下的 nginx。
#
#  用法： sudo bash rebuild_nginx.sh [--no-reload]
#         NGINX_SRC=/path NGINX_PREFIX=/opt/nginx sudo -E bash rebuild_nginx.sh
# ============================================================================
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NGINX_SRC="${NGINX_SRC:-$(cd "$SCRIPT_DIR/../../.." && pwd)}"   # 默认：本脚本位于 <nginx源码>/src/modules/nginx_db/，上溯三级即源码根
NGINX_PREFIX="${NGINX_PREFIX:-/opt/nginx}"
BIN="$NGINX_PREFIX/sbin/nginx"
JOBS="$(nproc 2>/dev/null || echo 2)"
RELOAD=1
[ "${1:-}" = "--no-reload" ] && RELOAD=0

C_G=$'\033[32m'; C_Y=$'\033[33m'; C_R=$'\033[31m'; C_0=$'\033[0m'
log(){ echo "${C_G}[rebuild]${C_0} $*"; }
warn(){ echo "${C_Y}[ WARN ]${C_0} $*"; }
die(){ echo "${C_R}[ERROR]${C_0} $*" >&2; exit 1; }
trap 'echo "${C_R}[ERROR]${C_0} 脚本执行失败(第 $LINENO 行)" >&2' ERR

[ "$(id -u)" -eq 0 ] || die "请以 root 运行：sudo bash $0"

[ -f "$NGINX_SRC/Makefile" ] || [ -f "$NGINX_SRC/objs/Makefile" ] \
  || die "找不到 nginx 构建目录：$NGINX_SRC（需先 ./configure）"
MOD="$NGINX_SRC/src/modules/nginx_db/postgres/ngx_http_postgres_module.c"
[ -f "$MOD" ] || die "找不到 postgres 模块源码：$MOD"
grep -q "ngx_http_read_client_request_body" "$MOD" \
  || warn "模块源码中未发现请求体读取修复，请确认已应用补丁"

log "==== [1/5] 编译 nginx (jobs=$JOBS) ===="
cd "$NGINX_SRC"
make -j"$JOBS"
[ -x "$NGINX_SRC/objs/nginx" ] || die "编译未生成 objs/nginx"

log "==== [2/5] 校验新二进制 ===="
"$NGINX_SRC/objs/nginx" -v

log "==== [3/5] 备份并安装到 $BIN ===="
mkdir -p "$NGINX_PREFIX/sbin"
if [ -x "$BIN" ]; then
  cp -a "$BIN" "$BIN.bak.$(date +%Y%m%d%H%M%S)"
  log "旧二进制已备份: $BIN.bak.*"
fi
# 关键：不能直接 cp 覆盖“正在运行”的二进制(会报 Text file busy)，
# 先复制成 .new，再用 mv 原子 rename 替换。
cp -f "$NGINX_SRC/objs/nginx" "$BIN.new"
chmod 755 "$BIN.new"
mv -f "$BIN.new" "$BIN"

log "==== [4/5] 配置校验 ===="
"$BIN" -t

log "==== [5/5] 重启以加载新二进制 ===="
# 注意：nginx -s reload 只重载配置，不会替换 master 正在运行的二进制；
# 要真正加载新编译的 nginx，必须停止后重新启动。
if [ "$RELOAD" = "1" ]; then
  # 统一使用 systemctl 管理启停（nginx.service 由 install.sh 注册：
  # ExecStop=nginx -s stop，Type=forking + PIDFile，systemd 会等待进程真正退出，
  # 不会出现旧实例未退、新实例 bind 失败的问题）
  if ! command -v systemctl >/dev/null 2>&1; then
    die "系统无 systemctl，无法按 systemd 方式重启 nginx"
  fi
  # 注意：不要用 list-unit-files|grep 判断（pipefail 下 grep -q 提前退出会令
  # systemctl 收到 SIGPIPE 而整体返回非零，造成误判），直接查 unit 文件。
  if [ ! -f /etc/systemd/system/nginx.service ] && [ ! -f /usr/lib/systemd/system/nginx.service ] && [ ! -f /lib/systemd/system/nginx.service ]; then
    die "未找到 nginx.service（请先执行 install.sh 注册 systemd 服务）"
  fi
  # 先停 unit（active/failed 状态下会执行 ExecStop 优雅停止并等待退出）
  systemctl stop nginx 2>/dev/null || true
  sleep 1
  # 兜底：历史上由脚本直接启动的 nginx 进程不属于 systemd cgroup，
  # systemctl 停不掉；若 80/443 仍被占用则清理（正常情况下不会走到这里）
  if ss -ltn 2>/dev/null | grep -qE ':(80|443)[[:space:]]'; then
    warn "端口 80/443 仍被占用（多为历史手工启动的游离 nginx 进程），pkill 兜底清理"
    pkill -x nginx 2>/dev/null || true
    i=0; while pgrep -x nginx >/dev/null 2>&1 && [ "$i" -lt 20 ]; do sleep 0.5; i=$((i+1)); done
    if pgrep -x nginx >/dev/null 2>&1; then
      pkill -9 -x nginx 2>/dev/null || true
      sleep 1
    fi
    systemctl reset-failed nginx 2>/dev/null || true
  fi
  if systemctl start nginx; then
    log "nginx 已由 systemctl 启动（新二进制）"
  else
    die "systemctl start nginx 失败，排查：journalctl -u nginx --no-pager | tail -20"
  fi
else
  warn "跳过重启(--no-reload)"
fi

log "================ 完成：nginx 已更新为修复版 ================"

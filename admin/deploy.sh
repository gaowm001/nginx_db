#!/usr/bin/env bash
# ============================================================================
#  NGXDB 本地管理台（admin）一键部署
#    1. 后端 API 注册为 systemd 服务 ngxdb-admin（仅监听 127.0.0.1:8765）；
#       无 systemd/非 root 时以 nohup 方式拉起。
#    2. 验证 /api/ping 可访问。
#    说明：管理台页面由后端直接服务（admin/static），不再发布静态页到 nginx，
#       并清理 nginx 部署目录中的历史发布残留；访问入口仅 http://127.0.0.1:8765/。
#  用法：bash admin/deploy.sh   （可被 install.sh 以 root 调用，也可单独执行）
# ============================================================================
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NGINX_PREFIX="${NGINX_PREFIX:-/opt/nginx}"
DEPLOY_ROOT="${NGXDB_DEPLOY_DIR:-$NGINX_PREFIX/html/ngxdb}"   # 仅供 app.py 发布业务模块使用（NGXDB_DEPLOY_DIR）
ADMIN_PORT="${ADMIN_PORT:-8765}"
SERVICE_USER="${SUDO_USER:-$(id -un)}"
RUN_AS_ROOT=0
if [ "$(id -u)" -eq 0 ]; then RUN_AS_ROOT=1; fi

log(){ echo "[admin] $*"; }
ok(){ echo "[admin][OK] $*"; }
warn(){ echo "[admin][WARN] $*"; }

# ---- 1. 清理 nginx 部署目录中的管理台历史发布残留（管理台仅走 127.0.0.1:8765） ----
if [ -n "$DEPLOY_ROOT" ] && [ "$DEPLOY_ROOT" != "/" ] && [ -e "$DEPLOY_ROOT/admin" ]; then
  rm -rf "$DEPLOY_ROOT/admin"
  ok "已清理 nginx 侧历史发布: $DEPLOY_ROOT/admin"
fi

# ---- 2. 后端服务 ----
PY="$(command -v python3 || echo /usr/bin/python3)"
run_bg() { nohup "$PY" "$SCRIPT_DIR/app.py" >> "$SCRIPT_DIR/data/server.log" 2>&1 & }

if [ "$RUN_AS_ROOT" = "1" ] && command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ]; then
  cat > /etc/systemd/system/ngxdb-admin.service <<EOF
[Unit]
Description=NGXDB Local Admin Console (127.0.0.1 only)
After=network.target postgresql.service nginx.service

[Service]
Type=simple
User=$SERVICE_USER
WorkingDirectory=$SCRIPT_DIR
ExecStart=$PY $SCRIPT_DIR/app.py
Environment=ADMIN_PORT=$ADMIN_PORT
Environment=NGXDB_DEPLOY_DIR=$DEPLOY_ROOT
Restart=on-failure
RestartSec=3

[Install]
WantedBy=multi-user.target
EOF
  systemctl daemon-reload
  systemctl enable --now ngxdb-admin >/dev/null 2>&1 || systemctl restart ngxdb-admin
  ok "已注册并启动 systemd 服务: ngxdb-admin (User=$SERVICE_USER)"
else
  if command -v systemctl >/dev/null 2>&1 && systemctl list-unit-files 2>/dev/null | grep -q '^ngxdb-admin.service'; then
    systemctl restart ngxdb-admin 2>/dev/null || true
    ok "已通过 systemctl 重启 ngxdb-admin"
  else
    pkill -f 'admin/app.py' 2>/dev/null || true
    sleep 0.5
    mkdir -p "$SCRIPT_DIR/data"
    run_bg
    warn "未以 root/无 systemd：已用 nohup 后台拉起（重启机器后需重新执行本脚本，或安装 systemd 服务）"
  fi
fi

# ---- 3. 验证 ----
sleep 1.5
code="$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$ADMIN_PORT/api/ping" || echo 000)"
[ "$code" = "200" ] && ok "API 自检通过: http://127.0.0.1:$ADMIN_PORT/api/ping" \
                    || warn "API 自检失败(HTTP $code)，查看 $SCRIPT_DIR/data/server.log"
ok "管理台入口: http://127.0.0.1:$ADMIN_PORT/（仅本机直连，不经 nginx）"

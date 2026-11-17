#!/usr/bin/env bash
# NGXDB 等保整改一键部署（需 root 执行）：
#   sudo bash tools/secure_deploy.sh
# 内容：TLS 自签证书 + 安全加固 nginx.conf（HTTPS/限流/安全头/专用账号）+ 每日备份 cron
set -e
CONF=/opt/nginx/conf
SRC_DIR=$(cd "$(dirname "$0")" && pwd)

echo "[1/5] 生成/检查 TLS 证书..."
mkdir -p "$CONF/cert"
if [ ! -f "$CONF/cert/ngxdb.pem" ]; then
  openssl req -x509 -nodes -newkey rsa:2048 -days 3650 \
    -keyout "$CONF/cert/ngxdb.key" -out "$CONF/cert/ngxdb.pem" \
    -subj "/CN=localhost" -addext "subjectAltName=IP:127.0.0.1,DNS:localhost" 2>/dev/null
fi
chmod 600 "$CONF/cert/ngxdb.key"

echo "[2/5] 轮换连接账号 conn 口令..."
NEWPASS=$(openssl rand -hex 16)
su -s /bin/bash postgres -c "psql -d postgres -tA -c \"ALTER ROLE conn PASSWORD '$NEWPASS'\"" >/dev/null

echo "[3/5] 安装安全加固 nginx.conf..."
TS=$(date +%Y%m%d_%H%M%S)
[ -f "$CONF/nginx.conf" ] && cp -f "$CONF/nginx.conf" "$CONF/nginx.conf.bak-$TS"
RELOAD_TOKEN=$(openssl rand -hex 16)
sed -e "s/__CONN_PASS__/$NEWPASS/" -e "s/__RELOAD_TOKEN__/$RELOAD_TOKEN/" "$SRC_DIR/nginx.conf.template" > "$CONF/nginx.conf"
chmod 600 "$CONF/nginx.conf"

echo "[4/5] 校验并重启 nginx..."
if "$CONF/../sbin/nginx" -t 2>/dev/null || /opt/nginx/sbin/nginx -t; then
  systemctl restart nginx || { echo "[ERROR] systemctl restart nginx 失败，排查：journalctl -u nginx --no-pager | tail -20"; exit 1; }
else
  echo "nginx -t 校验失败，回滚旧配置"; cp -f "$CONF/nginx.conf.bak-$TS" "$CONF/nginx.conf"; exit 1
fi

echo "[5/5] 安装每日备份任务..."
sed "s#__SCRIPT__#$SRC_DIR/backup.sh#" "$SRC_DIR/ngxdb-backup.cron" > /etc/cron.d/ngxdb-backup
chmod 644 /etc/cron.d/ngxdb-backup
bash "$SRC_DIR/backup.sh"

echo "部署完成。验证：curl -k https://127.0.0.1/func/sysinfo/login ..."
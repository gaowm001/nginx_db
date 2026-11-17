#!/usr/bin/env bash
# ============================================================================
#  NGXDB · nginx 数据库连接账号由 ngxapp 切换为 conn（需 root 执行）
#
#  用法： sudo bash tools/switch_to_conn.sh [conn密码]
#         不传密码时读取 /tmp/conn_pass.txt（管理员会话生成的随机密码）
#
#  步骤：备份 nginx.conf → 改 postgresconn 为 user=conn → nginx -t → 重启
#        → 冒烟验证（无 token 期待 100005）→ 验证通过后删除 ngxapp
#  回滚：验证失败时保留 ngxapp 与配置备份（$CONF.bak-时间戳），可一键还原
# ============================================================================
set -Eeuo pipefail

CONF=/opt/nginx/conf/nginx.conf
NGINX=/opt/nginx/sbin/nginx
PASS="${1:-}"
[ -n "$PASS" ] || PASS=$(cat /tmp/conn_pass.txt 2>/dev/null || true)
[ -n "$PASS" ] || { echo "[ERROR] 未提供 conn 密码（传参 1 或存在 /tmp/conn_pass.txt）" >&2; exit 1; }
[ "$(id -u)" -eq 0 ] || { echo "[ERROR] 请以 root 执行：sudo bash $0" >&2; exit 1; }
[ -f "$CONF" ] || { echo "[ERROR] 未找到 $CONF" >&2; exit 1; }
grep -q 'user=ngxapp' "$CONF" || echo "[WARN] nginx.conf 中未发现 user=ngxapp（可能已切换，继续）"

TS=$(date +%Y%m%d_%H%M%S)
cp -a "$CONF" "$CONF.bak-$TS"

echo "[1/4] 更新 nginx.conf 连接串（user=conn）..."
sed -i -E "s#user=ngxapp password=[^\"]*#user=conn password=${PASS}#" "$CONF"
chmod 600 "$CONF"

echo "[2/4] nginx -t 校验..."
if ! "$NGINX" -t; then
  echo "[ERROR] 校验失败，回滚配置"; cp -a "$CONF.bak-$TS" "$CONF"; exit 1
fi

echo "[3/4] 重启 nginx..."
systemctl restart nginx || { echo "[ERROR] systemctl restart nginx 失败，排查：journalctl -u nginx --no-pager | tail -20"; exit 1; }

echo "[4/4] 冒烟验证（无 token 调 /func/sysinfo/help_query，期待 100005）..."
sleep 1
RESP=$(curl -sk -m 10 -X POST https://127.0.0.1/func/sysinfo/help_query -d 'a=1' || true)
case "$RESP" in
  *errorcode*) : ;;
  *) RESP=$(curl -s -m 10 -X POST http://127.0.0.1/func/sysinfo/help_query -d 'a=1' || true) ;;
esac
echo "响应: $RESP"
case "$RESP" in
  *100005*|*100012*)
    echo "验证通过：nginx 已用 conn 连库并成功调用函数。"
    echo "清理：删除原账号 ngxapp..."
    if su -s /bin/bash postgres -c "psql -d postgres -v ON_ERROR_STOP=1 -c \"DROP OWNED BY ngxapp; DROP ROLE ngxapp;\""; then
      echo "ngxapp 已删除。"
    else
      echo "[WARN] ngxapp 删除失败（不影响运行，可手工处理）" >&2
    fi
    echo "完成。连接串已写入 $CONF（600 权限）。"
    ;;
  *)
    echo "[ERROR] 验证失败！ngxapp 未删除。可回滚：cp $CONF.bak-$TS $CONF && systemctl reload nginx" >&2
    echo "       并查看 /opt/nginx/logs/error.log" >&2
    exit 1
    ;;
esac

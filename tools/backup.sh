#!/usr/bin/env bash
# NGXDB 数据库每日备份（等保：数据备份与恢复）
# 用法：由 /etc/cron.d/ngxdb-backup 以 root 调度；也可手工执行：sudo bash tools/backup.sh
set -e
BACKUP_DIR=/opt/ngxdb-backup
KEEP=14
STAMP=$(date +%Y%m%d_%H%M%S)

mkdir -p "$BACKUP_DIR"
chown postgres:postgres "$BACKUP_DIR" 2>/dev/null || true

# 自定义格式（-Fc）支持 pg_restore 按对象恢复；如需纯 SQL 可改用 -Fp
su -s /bin/bash postgres -c "pg_dump -Fc -f '$BACKUP_DIR/ngxdb_$STAMP.dump' postgres"

# 只保留最近 KEEP 份
ls -1t "$BACKUP_DIR"/ngxdb_*.dump 2>/dev/null | tail -n +$((KEEP+1)) | xargs -r rm -f
echo "backup done: $BACKUP_DIR/ngxdb_$STAMP.dump"

# 恢复方法（示例）：
#   pg_restore -U postgres -d postgres --clean --if-exists /opt/ngxdb-backup/ngxdb_YYYYmmdd_HHMMSS.dump
# 建议另将最新备份同步到异地/异机（等保：异地备份），例如：
#   rsync -a "$BACKUP_DIR"/ngxdb_*.dump backup@192.168.x.x:/data/ngxdb-backup/
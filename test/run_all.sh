#!/usr/bin/env bash
# ngxdb 一键测试脚本：重建隔离测试库 → 数据库层测试 → 网页层测试 → 汇总
# 用法: bash test/run_all.sh
set -u
cd "$(dirname "$0")/.." || exit 2
PROJ=$(pwd)
if [ -z "${PGPASSWORD:-}" ]; then
  echo "[ERROR] 缺少数据库密码：请先 export PGPASSWORD='<postgres用户密码>' 再运行" >&2
  exit 1
fi
export PGPASSWORD
PSQL=${PSQL_BIN:-psql}
H=${NGXDB_HOST:-127.0.0.1}
U=${NGXDB_USER:-postgres}
TDB=ngxdb_test
PASS_ALL=0

echo "========== [1/4] 重建隔离测试库 $TDB =========="
$PSQL -h "$H" -U "$U" -d postgres -c "DROP DATABASE IF EXISTS $TDB;" || exit 1
$PSQL -h "$H" -U "$U" -d postgres -c "CREATE DATABASE $TDB;" || exit 1
if $PSQL -h "$H" -U "$U" -d "$TDB" -v ON_ERROR_STOP=0 -f "$PROJ/extension/ngxdb_postgresql.sql" > /tmp/ngxdb_load.log 2>&1; then
  echo "测试库加载完成"
else
  echo "警告: 测试库加载有错误(见 /tmp/ngxdb_load.log 尾部):"
  tail -5 /tmp/ngxdb_load.log
fi
# dump 恢复后序列值可能未同步(nextval 与已有主键冲突)，此处统一拨到最大值
$PSQL -h "$H" -U "$U" -d "$TDB" -v ON_ERROR_STOP=1 -f "$PROJ/test/fix_sequences.sql" >/dev/null 2>&1 \
  && echo "序列已同步" || echo "警告: 序列同步失败"


echo
echo "========== [2/4] 数据库层测试(全部函数) =========="
python3 "$PROJ/test/test_ngxdb_db.py" --db "$TDB" --host "$H" --user "$U" --password "$PGPASSWORD"
RC_DB=$?

echo
echo "========== [3/4] 网页/接口层测试(nginx) =========="
python3 "$PROJ/test/test_ngxdb_web.py" --base "${NGXDB_BASE:-http://127.0.0.1}"
RC_WEB=$?

echo
echo "========== [4/4] 清理与总结 =========="
$PSQL -h "$H" -U "$U" -d postgres -c "DROP DATABASE IF EXISTS $TDB;" >/dev/null 2>&1 && echo "测试库 $TDB 已删除"
[ "$RC_DB" -ne 0 ] && PASS_ALL=1
[ "$RC_WEB" -ne 0 ] && PASS_ALL=1
if [ "$PASS_ALL" -eq 0 ]; then
  echo "结果: 全部通过 ✔"
else
  echo "结果: 存在失败用例 ✘ (数据库层rc=$RC_DB 网页层rc=$RC_WEB)"
fi
exit $PASS_ALL

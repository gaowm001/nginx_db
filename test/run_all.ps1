# NGXDB 一键测试 —— Windows 版：重建隔离测试库 → 数据库层测试 → 网页层测试
# 用法： powershell -NoProfile -ExecutionPolicy Bypass -File test\run_all.ps1 -PgPassword '<postgres口令>'
param(
    [string]$PgHost = '127.0.0.1',
    [string]$PgUser = 'postgres',
    [string]$PgPassword = $env:PGPASSWORD,
    [string]$DbName = 'postgres',
    [string]$TestDb = 'ngxdb_test',
    [string]$Base = ''          # 网页层目标，默认 https://127.0.0.1
)
$ErrorActionPreference = 'Stop'
if (-not $PgPassword) { Write-Error '缺少数据库口令：传 -PgPassword 或设置环境变量 PGPASSWORD'; exit 1 }
$env:PGPASSWORD = $PgPassword
$root = Split-Path -Parent $PSScriptRoot
$env:PGPASSWORD = $PgPassword

Write-Host '== [1/3] 重建隔离测试库 =='
& psql -h $PgHost -U $PgUser -d $DbName -c "DROP DATABASE IF EXISTS $TestDb;"
& psql -h $PgHost -U $PgUser -d $DbName -c "CREATE DATABASE $TestDb;"
& psql -h $PgHost -U $PgUser -d $TestDb -v ON_ERROR_STOP=0 -f (Join-Path $root 'extension\ngxdb_postgresql.sql') | Out-Null
& psql -h $PgHost -U $PgUser -d $TestDb -v ON_ERROR_STOP=1 -f (Join-Path $root 'test\fix_sequences.sql') | Out-Null

Write-Host '== [2/3] 数据库层测试 =='
$py = (Get-Command python -ErrorAction SilentlyContinue).Source
if (-not $py) { $py = (Get-Command python3 -ErrorAction SilentlyContinue).Source }
if (-not $py) { Write-Error '未找到 python/python3'; exit 1 }
& $py (Join-Path $root 'test\test_ngxdb_db.py') --db $TestDb --host $PgHost --user $PgUser --password $PgPassword
$rcDb = $LASTEXITCODE

Write-Host '== [3/3] 网页/接口层测试 =='
$webArgs = @((Join-Path $root 'test\test_ngxdb_web.py'), '--base', $(if ($Base) { $Base } else { 'https://127.0.0.1' }))
& $py @webArgs
$rcWeb = $LASTEXITCODE

if ($rcDb -ne 0 -or $rcWeb -ne 0) { Write-Error ("测试存在失败：db={0} web={1}" -f $rcDb, $rcWeb) }
Write-Host '全部测试通过'

# NGXDB 每日数据库备份 —— Windows 版（等保：数据备份与恢复）
# 用法：
#   手工执行： powershell -NoProfile -ExecutionPolicy Bypass -File tools\backup.ps1 -PgPassword '<postgres口令>'
#   计划任务： 由 tools\secure_deploy.ps1 注册为每日 02:00 的 SYSTEM 任务
# 恢复： pg_restore -h 127.0.0.1 -U postgres -d postgres --clean --if-exists <dump文件>
param(
    [string]$PgHost = '127.0.0.1',
    [string]$PgUser = 'postgres',
    [string]$PgPassword = '',
    [string]$DbName = 'postgres',
    [int]$Keep = 14,
    [string]$BackupDir = 'C:\ngxdb-backup',
    [string]$PgPasswordFile = 'C:\nginx\conf\pg_super.pass'
)
$ErrorActionPreference = 'Stop'
if (-not $PgPassword) {
    if (Test-Path $PgPasswordFile) { $PgPassword = (Get-Content $PgPasswordFile -TotalCount 1).Trim() }
}
if (-not $PgPassword) {
    Write-Error '缺少数据库口令：传 -PgPassword，或在 $PgPasswordFile 提供首行口令，或设置环境变量 PGPASSWORD'
    exit 1
}
$env:PGPASSWORD = $PgPassword
New-Item -ItemType Directory -Force -Path $BackupDir | Out-Null
$stamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$file = Join-Path $BackupDir ("ngxdb_{0}.dump" -f $stamp)
& pg_dump -h $PgHost -U $PgUser -Fc -f $file $DbName
if ($LASTEXITCODE -ne 0) { Write-Error 'pg_dump 执行失败'; exit 1 }
Get-ChildItem -Path $BackupDir -Filter 'ngxdb_*.dump' |
    Sort-Object LastWriteTime -Descending | Select-Object -Skip $Keep |
    ForEach-Object { Remove-Item $_.FullName -Force }
Write-Host ("backup done: {0}" -f $file)
# 建议将最新备份同步到异地/异机（等保：异地备份），例如：
#   robocopy $BackupDir \\备份机\ngxdb-backup /xo
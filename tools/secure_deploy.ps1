# NGXDB 等保整改一键部署 —— Windows 版（需管理员 PowerShell）
# 用法： powershell -NoProfile -ExecutionPolicy Bypass -File tools\secure_deploy.ps1
# 内容： 自签 TLS 证书 + 安全加固 nginx.conf（HTTPS/限流/安全头/conn 口令轮换）
#        + 每日备份计划任务（schtasks）
# 前提： C:\nginx 已部署 nginx（含 conf\nginx.conf.template 同源的模板）、psql/openssl 在 PATH
param(
    [string]$NginxDir = 'C:\nginx',
    [string]$PgHost = '127.0.0.1',
    [string]$PgSuperUser = 'postgres',
    [string]$PgSuperPassword = $env:PGPASSWORD,
    [string]$DbName = 'postgres'
)
$ErrorActionPreference = 'Stop'
if (-not $PgSuperPassword) { Write-Error '缺少 postgres 口令：传 -PgSuperPassword 或设置环境变量 PGPASSWORD'; exit 1 }
$env:PGPASSWORD = $PgSuperPassword

Write-Host '[1/5] 生成/检查 TLS 证书...'
$certDir = Join-Path $NginxDir 'cert'
New-Item -ItemType Directory -Force -Path $certDir | Out-Null
$pem = Join-Path $certDir 'ngxdb.pem'
$key = Join-Path $certDir 'ngxdb.key'
if (-not (Test-Path $pem)) {
    $openssl = Get-Command openssl -ErrorAction SilentlyContinue
    if ($openssl) {
        & openssl req -x509 -nodes -newkey rsa:2048 -days 3650 `
            -keyout $key -out $pem -subj '/CN=localhost' `
            -addext 'subjectAltName=IP:127.0.0.1,DNS:localhost' 2>$null
    } else {
        # 无 openssl 时退化为 .NET 自签证书（导出 PFX；nginx 需 pem/key 时建议安装 openssl）
        $c = New-SelfSignedCertificate -DnsName 'localhost' -CertStoreLocation 'Cert:\LocalMachine\My'
        $pfx = Join-Path $certDir 'ngxdb.pfx'
        [System.IO.File]::WriteAllBytes($pfx, $c.Export([System.Security.Cryptography.X509Certificates.X509ContentType]::Pfx))
        Write-Warning '未检测到 openssl：已导出 PFX，请自行转换为 pem/key，或安装 openssl 后重跑'
    }
}

Write-Host '[2/5] 轮换连接账号 conn 口令...'
$newPass = -join ((1..32) | ForEach-Object { '{0:x}' -f (Get-Random -Maximum 16) })
$env:PGPASSWORD = $PgSuperPassword
& psql -h $PgHost -U $PgSuperUser -d $DbName -tA -c ("ALTER ROLE conn PASSWORD '{0}'" -f $newPass) | Out-Null
if ($LASTEXITCODE -ne 0) { Write-Error 'conn 口令轮换失败（确认角色 conn 已存在）'; exit 1 }

Write-Host '[3/5] 安装安全加固 nginx.conf...'
$tpl = Join-Path $PSScriptRoot 'nginx.conf.template'
$conf = Join-Path $NginxDir 'conf\nginx.conf'
$ts = Get-Date -Format 'yyyyMMdd_HHmmss'
if (Test-Path $conf) { Copy-Item $conf "$conf.bak.$ts" -Force }
$content = (Get-Content $tpl -Raw)
$content = $content.Replace('__CONN_PASS__', $newPass)
$reloadToken = -join ((1..32) | ForEach-Object { '{0:x}' -f (Get-Random -Maximum 16) })
$content = $content.Replace('__RELOAD_TOKEN__', $reloadToken)
# 证书路径改为绝对路径，避免 prefix 解析歧义
$content = $content.Replace('cert/ngxdb.pem', ($pem -replace '\\', '/'))
$content = $content.Replace('cert/ngxdb.key', ($key -replace '\\', '/'))
[System.IO.File]::WriteAllText($conf, $content)   # UTF-8 无 BOM（nginx 不接受 BOM）

Write-Host '[4/5] 校验并重启 nginx...'
$exe = Join-Path $NginxDir 'nginx.exe'
& $exe -t -c $conf -p "$NginxDir\"
if ($LASTEXITCODE -ne 0) { Write-Error 'nginx -t 校验失败，已保留旧配置'; exit 1 }
& $exe -s stop 2>$null
Start-Sleep -Seconds 1
Start-Process -FilePath $exe -ArgumentList @('-c', $conf, '-p', "$NginxDir\") -WorkingDirectory $NginxDir -WindowStyle Hidden
Start-Sleep -Seconds 2

Write-Host '[5/5] 注册每日备份计划任务...'
$backupScript = Join-Path $PSScriptRoot 'backup.ps1'
# 将 postgres 口令写入仅 SYSTEM/管理员可读的文件，供计划任务中的 backup.ps1 使用
$passFile = Join-Path $NginxDir 'conf\pg_super.pass'
[System.IO.File]::WriteAllText($passFile, $PgSuperPassword)
icacls $passFile /inheritance:r /grant:r 'SYSTEM:F' /grant:r 'Administrators:F' | Out-Null
schtasks /Create /F /TN 'NGXDB Backup' /SC DAILY /ST 02:00 /RU SYSTEM `
    /TR "powershell -NoProfile -ExecutionPolicy Bypass -File `"$backupScript`"" | Out-Null

Write-Host '部署完成。验证：'
Write-Host '  curl.exe -k https://127.0.0.1/func/sysinfo/login ...   (http 会 301 跳转 https)'
Write-Host '  Get-ChildItem C:\ngxdb-backup'
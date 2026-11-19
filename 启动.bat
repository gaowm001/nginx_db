@echo off
chcp 65001 >nul
setlocal EnableExtensions
cd /d "%~dp0"
title NGXDB 本地服务启动器
rem 让 admin 能找到 psql 与 pgrep 垫片；PYTHONHOME 供 plpython3u 使用
set "PATH=%~dp0pgsql\bin;%~dp0nginx_db_src\admin;%PATH%"
set "PYTHONHOME=%~dp0pgsql\python313"

echo ================================================
echo   NGXDB 一键启动  (目录: %~dp0)
echo ================================================

echo [1/3] PostgreSQL ...
pg_ctl status -D "%~dp0pgsql\data" 2>nul | findstr /c:"running" >nul
if errorlevel 1 (
  echo   正在启动 PostgreSQL ...
  pg_ctl start -D "%~dp0pgsql\data" -l "%~dp0pgsql\pg.log" -o "-p 5432 -c listen_addresses=127.0.0.1" >nul 2>&1
  ping -n 4 127.0.0.1 >nul
)
pg_ctl status -D "%~dp0pgsql\data" 2>nul | findstr /c:"running" >nul
if errorlevel 1 (echo   [警告] PostgreSQL 未能启动，详见 pgsql\pg.log) else (echo   PostgreSQL 运行中)

echo [2/3] 管理台 admin ...
netstat -ano | findstr /r /c:":8765 .*LISTENING" >nul 2>&1
if errorlevel 1 (
  echo   正在启动 admin ...
  start "NGXDB-admin" /min py -3 -u "%~dp0nginx_db_src\admin\app.py"
  ping -n 3 127.0.0.1 >nul
)
netstat -ano | findstr /r /c:":8765 .*LISTENING" >nul 2>&1
if errorlevel 1 (echo   [警告] admin 未能启动) else (echo   admin 运行中: http://127.0.0.1:8765/)

echo [3/3] nginx ...
tasklist /FI "IMAGENAME eq nginx.exe" 2>nul | find /i "nginx.exe" >nul
if errorlevel 1 (
  echo   正在启动 nginx ...
  start "NGXDB-nginx" /min /D "%~dp0nginx" "%~dp0nginx\nginx.exe"
  ping -n 3 127.0.0.1 >nul
)
tasklist /FI "IMAGENAME eq nginx.exe" 2>nul | find /i "nginx.exe" >nul
if errorlevel 1 (echo   [警告] nginx 未能启动，详见 nginx\logs\error.log) else (echo   nginx 运行中)

echo.
echo   系统入口: http://127.0.0.1/login.html   (账号 admin / 123456)
echo   管理台  : http://127.0.0.1:8765/
echo.
start "" http://127.0.0.1/login.html
start "" http://127.0.0.1:8765/
timeout /t 5 >nul

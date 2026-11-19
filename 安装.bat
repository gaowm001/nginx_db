@echo off
chcp 65001 >nul
cd /d "%~dp0"
title NGXDB 一键安装
net session >nul 2>&1
if errorlevel 1 (
  echo 需要管理员权限，正在弹出授权窗口 ...
  powershell -NoProfile -Command "Start-Process -FilePath '%~f0' -Verb RunAs"
  exit /b
)
echo ================================================
echo   NGXDB 一键安装（GUI 模式，请在窗口中点击"开始安装"）
echo ================================================
py -3 "%~dp0install.py"
echo.
echo 安装进程已退出，完整日志见 install.log
pause

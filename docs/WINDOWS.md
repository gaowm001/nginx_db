# NGXDB Windows 环境支持手册

本手册由 README.md 拆分而来（2026-11-13），内容随主 README 演进同步维护。

## 一、Windows 环境支持

项目脚本分为三类，Windows 下的可用性如下：

### 1.1 跨平台 Python 工具（直接可用）

| 脚本 | 说明 |
|---|---|
| `tools/gen_module.py` | 生成器本体跨平台；`--deploy` 的目标目录默认 Linux 为 `/opt/nginx/html/ngxdb`、Windows 为 `C:\nginx\html\ngxdb`，均可用环境变量 `NGXDB_DEPLOY_DIR` 覆盖。执行 `--apply/--apply-tx` 需 `psql` 在 PATH。 |
| `test/test_ngxdb_db.py` | 通过 psql 子进程执行，Windows 需 psql 在 PATH；口令用环境变量 `PGPASSWORD` 或 `--password` 传入。 |
| `test/test_ngxdb_web.py` | 默认目标 `https://127.0.0.1`（自动忽略自签证书）；也可 `--base http://...` 指向未启用 TLS 的实例。 |

### 1.2 PowerShell 工具（Windows 等价实现）

| Linux 版 | Windows 版 | 用途 |
|---|---|---|
| `tools/backup.sh`（cron 每日 2 点） | `tools/backup.ps1`（schtasks 每日 2 点，SYSTEM 运行） | pg_dump 备份，本地保留 14 份 |
| `tools/secure_deploy.sh` | `tools/secure_deploy.ps1` | 自签证书（openssl 或 .NET 自签）、轮换 conn 口令、安装安全 nginx.conf、`nginx -t` 校验后重启、注册备份计划任务 |
| `test/run_all.sh` | `test/run_all.ps1` | 重建隔离测试库 → 数据库层测试 → 网页层测试 |

Windows 快速执行（管理员 PowerShell）：

```powershell
cd <项目目录>
powershell -NoProfile -ExecutionPolicy Bypass -File tools\secure_deploy.ps1 `
    -NginxDir C:\nginx -PgSuperPassword '<postgres口令>'
powershell -NoProfile -ExecutionPolicy Bypass -File test\run_all.ps1 -PgPassword '<postgres口令>'
```

要点：
- nginx.conf 由 `tools/nginx.conf.template` 生成，`__CONN_PASS__` 替换为随机口令，
  证书路径改写为绝对路径；写入采用**无 BOM UTF-8**（nginx 不接受 BOM）。
- postgres 超级用户口令写入 `C:\nginx\conf\pg_super.pass`，ACL 限制为
  SYSTEM/Administrators，仅供计划任务中的备份脚本读取。
- 计划任务以 SYSTEM 身份每日 02:00 运行 `backup.ps1`。

### 1.3 Windows 限制与说明

1. `install.sh`（apt/systemd 编译安装）与 `rebuild_nginx.sh`（gcc 重新编译 nginx）
   为 **Linux 专用**；Windows 下请使用 nginx 官方 Windows 二进制，postgres 模块需
   自行用 MSVC 编译（或使用已含该模块的构建）。
2. nginx 官方对 Windows 的定位是开发/演示；生产环境仍建议 Linux 部署。
3. `sudo`/`systemctl`/`cron` 相关逻辑在 Windows 分别由管理员权限、`nginx -s stop`
   + 进程启动、`schtasks` 计划任务替代。
4. 自签证书在浏览器同样会提示不受信任，生产请更换 CA 证书。

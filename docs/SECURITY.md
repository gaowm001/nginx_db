# NGXDB 安全与等保 2.0 合规手册

本手册由 README.md 拆分而来（2026-11-13），内容随主 README 演进同步维护。

## 一、等级保护 2.0 合规检查与整改（2026-11-13）

对照《网络安全等级保护基本要求》（GB/T 22239-2019）技术要求逐项自查，结果如下。

### 1.1 检查前已符合的控制点

| 等保要求 | 系统现状 |
|---|---|
| 登录失败处理（限制非法登录次数） | 10 分钟内失败>5 次锁定 10 分钟、3 小时内>10 次锁定 3 小时 |
| 会话超时自动退出 | token 有效期 180 分钟，滑动续期，过期返回 100006 |
| 会话并发控制 | 单人登录顶号（tokentype） |
| 访问控制（RBAC 到操作级） | sysaction 权限树 + operpermission/角色，函数入口强制 check_login；**系统内置操作员（operatorid 10000/100000）禁止删除**（`operinfo_del`/`operinfoorg_del`/`operinfopermission_del` 返回 100036），防止误删后系统无法登录 |
| SQL 注入防护 | 全程参数化，注入实测无效 |
| 错误信息保护 | 统一错误码，不泄露堆栈/SQL |
| 身份标识唯一 | 账号唯一、token 与操作员绑定 |

### 1.2 本轮已整改项（代码/配置已落地并验证）

| # | 等保要求 | 整改前问题 | 整改措施 | 验证结果 |
|---|---|---|---|---|
| 1 | 身份鉴别-口令存储 | operinfo.pass **明文存储**（如 123456） | 全部改为 blowfish 单向哈希（crypto.crypt），登录/改密用 `crypt(输入,存量哈希)` 比对；存量明文已迁移；新增 BEFORE 触发器保证今后任何写入路径自动哈希 | 旧口令仍可登录（0）；库中 pass 全部为 `$2a$08$` 哈希 |
| 2 | 安全审计-日志含敏感信息 | loginlog **记录明文口令** | 删除 loginlog.pass 列，登录日志不再含口令 | loginlog 已无 pass 列 |
| 3 | 安全审计-操作审计 | serverlog 有表无数据，重要操作无审计 | 新增审计触发器：public.loginlog/operinfo/sysaction/roleinfo/systeminfo/operpermission/rolepermission 的增删改自动写入 public.serverlog（时间、客户端IP、对象、操作类型、数据快照、操作者，pckname='audit'）；check_login 成功后通过会话变量向审计提供操作者 | 实测登录/更新后 serverlog 出现 audit 记录 |
| 4 | 通信传输保密性 | 仅 HTTP 明文（口令、token 裸奔） | nginx 启用 TLSv1.2/1.3（自签证书，生产换 CA 证书），HTTP 301 跳转 HTTPS；增加 X-Frame-Options/nosniff/Referrer-Policy 头、server_tokens off | 见 tools/secure_deploy.sh 部署后验证 |
| 5 | 最小权限 | nginx 以 postgres **超级用户**连库，口令明文在 conf | 连接账号改用既有角色 `conn`（`public.serverlog`/`public.loginlog` INSERT 已授），仅授予函数 EXECUTE（无业务表读写权限）；原专用账号 `ngxapp` 已删除，密码随机并轮换 | 新库导入后 conn 可连；直查业务表应被拒 |
| 6 | 身份鉴别-口令复杂度 | 可设 123456 等弱口令 | 改密/设密强制 ≥8 位且含字母+数字，拒绝常见弱口令；新增错误码 100035 | 弱口令返回 100035 |
| 7 | 数据备份 | 无自动备份 | tools/backup.sh：每日 pg_dump 自定义格式，本地保留 14 份，附恢复命令与异地同步示例；cron 每日 2 点执行 | 脚本试运行生成 dump 文件 |
| 8 | 边界-资源控制 | 无接口限流 | nginx limit_req（50r/s，burst 100）+ limit_conn，防暴力破解/资源耗尽 | 部署后超频请求被拒 |

### 1.3 部署侧整改执行方法

数据库层整改已随 dump 应用；nginx 层（TLS/限流/conn 口令轮换/备份 cron）需要 root，
已封装为一键脚本：

```bash
sudo bash tools/secure_deploy.sh          # Linux
# Windows（管理员 PowerShell）：
# powershell -NoProfile -ExecutionPolicy Bypass -File tools\secure_deploy.ps1
# 执行内容：生成自签证书 → 轮换 conn 口令 → 安装安全 nginx.conf（tools/nginx.conf.template）
#          → nginx -t 校验后重启（失败自动回滚） → 注册每日备份计划任务 → 试运行备份
# 验证：curl -k https://127.0.0.1/func/sysinfo/login ... （原 http 地址 301 跳转 https）
```

### 1.4 仍需管理/环境层面落实的事项

1. **双因素认证**：等保三级要求两种以上鉴别技术组合，当前仅口令；建议按定级增加
   OTP/证书/短信因子。
2. **三权分立**：系统管理员、审计管理员、安全管理员应分设账号（RBAC 已支持按
   权限点拆分，需建立管理制度并分别建号授权）。
3. **证书管理**：当前为自签证书，生产应更换为机构 CA 签发并纳入证书有效期监控。
4. **异地备份与恢复演练**：backup.sh 已附 rsync 异地同步示例，需落实目的端并定期
   演练 pg_restore 恢复。
5. **日志留存制度**：等保要求日志留存不少于 6 个月，需配套归档存储与定期核查流程。
   日志已统一到 public（serverlog/loginlog）并按月分区就绪（docs/PERFORMANCE.md 2.5），
   建议按"当月主表 + 历史月分区 + 6 个月后归档导出"落地留存。
6. **入侵检测与告警**：登录锁定、越权尝试等事件目前仅入库，建议接入监控告警
   （如日志平台关键字告警）。
7. **漏洞与补丁管理**：nginx/PostgreSQL 应纳入定期漏洞扫描与升级流程。
8. **物理环境、网络边界设备策略**等属于机房与网络层，需按部署环境另行评估。

### 1.5 局限性说明

- serverlog.clientip 记录的是 **PG 看到的连接地址**（本机部署时为 127.0.0.1），
  真实客户端 IP 以 nginx access log 为准；如需在审计行中记录真实 IP，可由
  nginx 将 `$remote_addr` 作为参数传入业务函数后再写入。
- 自签证书仅消除明文传输，浏览器会提示不受信任；生产请更换 CA 证书。
- nginx 与 PostgreSQL 之间为本机回环连接（未启用 PostgreSQL 端 TLS）；
  同机部署下风险可控，**跨机部署时**应为 PostgreSQL 启用 TLS 或改用
  stunnel/私有网络，并将 `postgresconn` 指向加密链路。
- `/_reloadfunctions`、`/_poolctl` 管理接口在未配置 `postgresreloadtoken` 时
  仅允许 127.0.0.1 调用；生产环境建议配置随机 token（`tools/secure_deploy.sh`
  会自动生成并写入），避免同机其他进程误触。当前版本决策：**保持仅本机限制、
  默认不启用 token**（方便试用；token 机制已内置，待实践检验后按反馈调整）。
- 审计触发器覆盖核心权限/员工/系统表；新增业务表如需审计，可复用
  `gm.audit_trigger()` 为业务表创建同样的 AFTER 触发器。

### 1.6 等保复查记录（2026-11-13）

对 15.1–15.5 各控制项做落地复核，并完成以下整改：

| 控制项 | 本轮动作 | 复核结果 |
|---|---|---|
| 身份鉴别-口令存储 | 全量复核 `operinfo.pass` | ✅ 全部为 blowfish 哈希（`$2a$08$`），明文 0 行；复杂度错误码 100035 在册 |
| 身份鉴别-登录日志 | 复核 `loginlog` 结构 | ✅ 已无 pass 列，日志不含口令 |
| 安全审计-操作审计 | 复核 `trg_audit` 触发器 | ✅ 7 张核心表（public.loginlog/operinfo/sysaction/roleinfo/systeminfo/operpermission/rolepermission）均已挂 `gm.audit_trigger`，审计写入 public.serverlog（pckname='audit'） |
| 安全审计-请求日志 | 修复 `public.serverlog` 授权缺口（此前连接账号无 INSERT 权限，模块写日志一直静默失败） | ✅ 每次调用记录 clientip/funcname/params/res/operatorid，实测写入成功 |
| 日志归拢 | 三张日志表统一到 public（`public.serverlog` 请求日志+操作审计、`public.loginlog` 登录日志），sysinfo 侧旧表保留只读、数据已 +100000 平移迁入；分区按月就绪 | ✅ 迁移幂等重放通过；`serverlog_query`/`loginlog` 写入均走 public 表 |
| 最小权限-连接账号 | nginx 连接账号由 `ngxapp` 切换为既有角色 `conn`（仅函数 EXECUTE + serverlog INSERT/序列，无业务表读写；conn 原 `CONNECTION LIMIT=0` 已解除）；`ngxapp` 已 `DROP OWNED`+`DROP ROLE` 删除 | ✅ 行为冒烟验证通过 |
| 访问控制-暴露面收敛 | 接口文档 `sysinfo.help_query` 排除 `crypto`/`gm`（仅业务 schema，70 个函数）；nginx postgres 模块加载 SQL 同步排除 `gm`（`ngx_http_postgres_module.c` 两处，与 help 一致） | ✅ 已重编译重启（error.log：`find/translate 70 functions`），nginx 加载清单与接口文档一致；`/func/gm/*` 已不可路由（连接直接断开），业务接口无回归 |
| 数据备份 | 复核 `/etc/cron.d/ngxdb-backup`（每日 2 点，pg_dump -Fc，保留 14 份） | ✅ cron 已安装 |

> 说明：`crypto`=pgcrypto 扩展函数、`gm`=框架内部函数（check_login/returnjson/http_* 等），
> 二者均不对外展示、不经 `/func/` 路由暴露。gm 全部 12 个函数的 `COMMENT ON FUNCTION` 说明已补全。
> 日志三表（已统一归 public）：`public.serverlog`（模块请求日志 + 操作审计，pckname='audit' 为审计行）+
> `public.loginlog`（登录日志，不含口令）。
> 查询入口：`sysinfo.serverlog_query`（权限 107，查 public.serverlog）与 `sysinfo.loginlog_query`（查 public.loginlog）；
> 等保"日志留存≥6 个月"由按月分区+归档承接（docs/PERFORMANCE.md 2.5）。

管理层面遗留事项（双因素、三权分立、CA 证书、异地备份演练、日志留存 6 个月、告警接入等）仍见 1.4。

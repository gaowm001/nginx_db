# NGXDB 性能手册：实测、瓶颈与扩展

> 本文所有数字均为**本项目在真实部署上实测**得到，并给出复现脚本与环境参数。
> 你的硬件、数据量、TLS 配置不同，绝对值会有差异，**看趋势与瓶颈排序比看单点数字更有意义**。

## 一、实测

### 1.1 测试环境（请对照自己的环境换算）

| 项 | 值 |
|---|---|
| CPU / 内存 | 4 vCPU / 7 GB（VMware 虚拟机） |
| nginx | 1.30.6 + `ngx_http_postgres_module`，**`worker_processes 1`**、`worker_connections 1024` |
| PostgreSQL | 18.6，`max_connections=100`、`shared_buffers=128MB`、`work_mem=4MB` |
| 连接方式 | HTTPS（TLSv1.2，自签证书），本机回环 `127.0.0.1` |
| 连接账号 | `conn`（最小权限），模块连接池 `postgrespoolsize 16` |
| 数据量 | 19 张业务表，库总大小 13 MB；`public.serverlog` 约 1.5 千行 / 5.3 MB |
| 函数清单 | 业务函数 70 个（`gm` 内部函数不加载、不暴露） |

请求链路：`客户端 ──HTTPS──> nginx 模块 ──libpq──> PG 函数(含 gm.check_login 两层解密) ──> JSON`

**每个请求都包含完整的鉴权开销**（两层 AES 解密 + 多表校验 + 审计写入），没有为了好看而绕过鉴权。

### 1.2 复现方法

延迟（串行 25 次，取分布）：

```python
import json, ssl, time, urllib.request, urllib.parse, statistics
BASE = 'https://127.0.0.1'
ctx = ssl.create_default_context(); ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE

def api(path, d):
    body = urllib.parse.urlencode(d).encode()
    r = urllib.request.Request(BASE + '/func/' + path, data=body,
        headers={'Content-Type': 'application/x-www-form-urlencoded'}, method='POST')
    with urllib.request.urlopen(r, timeout=20, context=ctx) as resp:
        return json.loads(resp.read().decode())

tok = api('sysinfo/login', {'loginname': 'admin', 'pass': '123456', 'system': '100'})['info']['token']
xs = []
for _ in range(25):
    t = time.perf_counter(); api('sysinfo/errorcode_query', {'token': tok})
    xs.append((time.perf_counter() - t) * 1000)
xs.sort()
print('avg=%.2f p50=%.2f p95=%.2f max=%.2f ms' % (
    statistics.mean(xs), xs[len(xs)//2], xs[int(len(xs)*.95)-1], xs[-1]))
```

并发：多线程各发 N 个请求，统计 TPS 与延迟分位；**注意先用
`curl -sk 'https://127.0.0.1/func/_poolctl'` 观察池状态，并留意是否触发限流**（见 1.4）。

### 1.3 单请求延迟（2026-11-14 实测）

| 接口 | 说明 | avg | p50 | p95 | max |
|---|---|---|---|---|---|
| `sysinfo/operinfopermission_query` | 权限表联查 | **2.41 ms** | 2.37 | 2.71 | 2.87 |
| `sysinfo/sysorg_query` | 部门树（递归 CTE + 路径计算） | **2.38 ms** | 2.29 | 2.57 | 3.41 |
| `sysinfo/operinfo_query` | 操作员分页（联查组织/角色） | **2.91 ms** | 2.56 | 2.98 | 9.48 |
| `sysinfo/serverlog_query` | 日志查询（大表 + 联查姓名） | **3.07 ms** | 2.91 | 3.87 | 4.80 |
| `sysinfo/loginlog_query` | 登录日志查询 | **3.16 ms** | 3.14 | 4.02 | 4.49 |
| `sysinfo/actions_query` | 权限点列表 | **3.48 ms** | 3.16 | 4.68 | 6.08 |
| `sysinfo/help_query` | **接口文档**：一次性返回 70 个函数的全部元数据 | **4.23 ms** | 3.93 | 5.44 | 7.52 |
| `sysinfo/errorcode_query` | 错误码普通分页 | **4.44 ms** | 3.13 | 13.38 | 16.06 |
| `sysinfo/login` | 登录：**bcrypt 口令校验 + 两层 AES 生成 token + 写库** | **21.2 ms** | 21.2 | 22.0 | 22.7 |

读数要点：

1. **普通 CRUD 查询稳定在 2~4 ms**，包含 TLS、鉴权解密与审计写库。人眼感知阈值约 100 ms，
   余量充足。
2. **`login` 的 21 ms 基本都是 bcrypt（blowfish）口令哈希的计算成本**，这是等保"口令不可逆
   存储"的必然代价，不是架构开销。它的量级由 `crypt()` 的 cost 决定，与数据量无关；
   登录是低频操作（每人每天几次），无需优化。
3. `errorcode_query` 的 p95 抖动较大（13 ms），与其表小、走顺序扫描 + 全量 `array_agg`
   拼 JSON 有关；在 2.5 加索引后应稳定到 3 ms 级。**这类"小表大 JSON"接口是延迟抖动
   的主要来源，优化点是索引与返回列裁剪，而不是加机器。**
4. `help_query` 在返回 70 个函数完整元数据（含注释、参数说明）的情况下仍只要 4 ms——
   因为它是一次性 `pg_proc` 元数据聚合，不在热路径上。


### 1.4 并发与吞吐（含一个关键发现）

在 `limit_req zone=apilimit rate=50r/s burst=100 nodelay` 的**放行窗口内**压测：

| 并发 | 请求数 | 成功 | 总耗时 | 吞吐 | avg | p95 | max |
|---|---|---|---|---|---|---|---|
| 10 | 100 | 100 | 0.208 s | **482 req/s** | 19.7 ms | 39.9 ms | 45.7 ms |
| 20 | 100 | 100 | 0.175 s | **573 req/s** | 30.7 ms | 35.5 ms | 35.9 ms |

持续速率（按 40 req/s 压 5 秒，不触发限流）：

| 目标速率 | 请求 | 成功 | 失败 | 实测速率 | avg | p95 | p99 |
|---|---|---|---|---|---|---|---|
| 40 req/s | 197 | 197 | **0** | 39.3 req/s | 6.39 ms | 8.61 ms | 15.57 ms |

> ⚠️ **重要发现：单机当前的并发天花板是 nginx 限流配置，不是程序处理能力。**
> 实测中按 50 并发无节制打请求，会得到大量 `HTTP 503`，`error.log` 明确记录
> `limiting requests, excess: 100.900 by zone "apilimit"`——这是**等保要求的限流在生效**，
> 属于预期行为，不是缺陷。程序侧在被允许的窗口内可以跑出 **570+ req/s**。
>
> 这意味着：**扩容前先想清楚你的限流策略**。压测/容量规划时要么在测试环境调大
> `rate`，要么用多来源 IP（`limit_req_zone` 按 `$binary_remote_addr` 计数）压测，
> 否则测到的是限流，不是系统极限。

**并发下延迟上升的解释**（avg 从 2~4 ms 升到 20~30 ms）：检测环境为
`worker_processes 1`，即**单 worker 串行处理所有请求**，并发请求只能在 socket 队列与
连接池里排队。这是本项目当前配置的刻意选择（部署简单、无竞态），也是**第一个该调的参数**
（见 2.2）。

### 1.5 连接池是否真的在工作

`/_poolctl` 实测输出（节选）：

```json
{"errorcode":0,"ctxs":[{"ctx":0,
  "conn":"host=127.0.0.1 dbname=postgres user=conn password=***",
  "functions":70,"target":16,"idle":1,"inuse":0,
  "get":1477,"hit":1476,"new":1,"fail":0,"drop":0,"gen":0}]}
```

- `get=1477 / hit=1476 / new=1`：累计 1477 次取连接，**只有 1 次真正建连**，
  命中率 99.93%；
- `PG` 侧 `pg_stat_activity` 中 `conn` 角色连接数**恒为 1**，而请求早已上千——
  验证了"请求级连接复用"确实生效；
- `fail=0`：无建连失败；`drop=0`：无被丢弃的连接。

对照：**改造前**每个请求都要 `PQconnectdb/PQfinish`（TCP 握手 + 认证），
连接数随并发线性膨胀，是最先撞墙的地方；现在单机只需 1 条常驻连接。

### 1.6 结论

| 维度 | 结论 |
|---|---|
| 单请求 | 2~4 ms（不含登录），远低于感知阈值 |
| 吞吐 | 限流窗口内 570+ req/s；**当前实际上限由 `limit_req` 决定** |
| 横向 | 状态全在 PG，实例无状态，可水平加节点 |
| 纵向 | `worker_processes`、`worker_connections`、池深、PG 参数均有余量 |
| 真正的瓶颈排序 | **① 限流配置 → ② 单 worker 串行 → ③ PG 实例 → ④ 日志表增长** |


## 二、高并发与海量数据：现状与路线

> 与框架型方案文档不同，本章区分三件事：**已内置的**（拿来就用）、**配置就能调的**
> （改 nginx.conf）、**需要额外组件的**（PDBouncer/Patroni 等）。先说清哪些已经不用做。

### 2.1 已经内置的扩展能力（无需开发）

| 能力 | 实现 | 怎么用 |
|---|---|---|
| 请求级连接池 | 模块内每 worker 空闲连接栈，归还时 `ROLLBACK` 失败事务 + 重置 `app.operatorid` + 健康检查 | `postgrespoolsize N`（默认 16，`0` 关闭） |
| 池动态伸缩 | `target` 是"空闲保留目标"：高峰池空即新建（借出不受限），低谷超 target 归还即关 | 无需配置，天然跟随负载 |
| 池热管理 | `/_poolctl` 支持 `status`/`resize`/`on`/`off`，写共享内存页、**全 worker 立即生效** | 见下 |
| 函数清单热重载 | `/_reloadfunctions`，跨 worker 代数计数器，不中断在线流量 | 改完存储过程即生效 |
| 按入口隔离 | 每个 `postgresconn` location 一套函数表 + 连接池 + 统计（≤16 个入口） | 单 nginx 可挂多个库 |
| 实例无状态 | token/权限/审计全在 PG，进程内只有函数表和池 | 直接加机器 |

池热管理的实际用法（生产上把 `target` 当"油门"用）：

```bash
curl -sk "https://127.0.0.1/func/_poolctl?token=$TOK"              # 看状态与统计
curl -sk -X POST "https://127.0.0.1/func/_poolctl?action=resize&conn=0&target=32&token=$TOK"
curl -sk -X POST "https://127.0.0.1/func/_poolctl?action=off&conn=0&token=$TOK"   # 排空
```

调参判据（**用统计说话，别猜**）：

- `new` 持续增长 → 池目标偏小，调大 `target`；
- `drop` 持续增长 → 池目标偏大，可调小省 PG 连接；
- `fail` 增长 → PG 侧连不上（`max_connections` 打满 / PG 重启 / 网络），看 PG 日志；
- `hit/get` 比值就是复用命中率，健康时应 >99%。

### 2.2 第一优先级：把 nginx 自身参数放到合适值

本项目默认配置是"能跑就行"的保守值，**扩容的第一步不是加机器，是改这几行**：

```nginx
worker_processes  auto;          # 当前模板是 1 —— 单 worker 串行是并发延迟主因
worker_connections 10240;        # 配合系统 ulimit -n
```

- `worker_processes` 与 CPU 核数对齐（4 核实测环境可提到 4 → 理论并发处理能力 ×4，
  注意每 worker 各自持有独立连接池，PG 总连接 ≈ worker 数 × `postgrespoolsize`）；
- `limit_req` / `limit_conn` 是**等保要求**，不要为了压测数字而随意关掉；生产应按
  "预期正常用户数 × 单用户请求频率"设定，并在 LB 层做全局配额；
- 静态资源与 `/func/` 分离 location，静态侧开 `gzip` + 强缓存（前端 JS/CSS 已带版本号）。

### 2.3 数据库连接：什么时候还需要 PgBouncer

内置池已解决单实例内的连接复用（实测建连次数从"每请求 1 次"降到"每实例 1 次"）。
**只有以下场景才需要再加 PgBouncer**：

- nginx 实例数 × worker 数 × 池深 > PG `max_connections` 的 70%；
- 需要连接级排队/超时统一治理；
- 多应用共享同一个 PG。

若引入 PgBouncer，模块侧只需把 `postgresconn` 指向它的端口，**零代码改动**。但注意：

> ⚠️ `gm.check_login` 用 `set_config('app.operatorid', ..., false)` 写的是**会话级**变量
> （`false` = 不随事务结束回滚），用于审计归属。若 PgBouncer 使用
> `pool_mode=transaction`，同一物理连接会在不同会话间复用，存在**审计串号**风险。
> 两种解法：① PgBouncer 用 `pool_mode=session`；② 把该变量改成事务级并确认审计读到的是
> 同事务值后再切换。

### 2.4 读写分离与多库

**读写分离**：业务函数自带事务（函数内 COMMIT），写入必须走主库；`*_query`、
`help_query` 等只读接口可分流到流复制备库。

- 落地方式：入口 nginx 按 URI 前缀分流（`/func/ro/...` → 指向备库的 location），
  或将只读接口部署在独立实例上；
- 注意 `login` / token 续期会写 `operinfo.tokentime`，**必须主库**；
- 备库有复制延迟：查询"刚写完立刻要读"的场景不要走备库。

**多库（垂直拆分）**：业务按 schema 隔离（`sysinfo`/`hr`/`fin`…），模块已支持
**单 nginx 挂多个库**——每个库一个 location：

```nginx
location /func {                       # 主业务库
    postgresconn "host=... dbname=erp  user=conn ...";
    postgrespoolsize 16;
    rewrite ^/func/(.*)$ /$1 break;
}
location /func2 {                      # 另一业务库
    postgresconn "host=... dbname=hr   user=conn ...";
    postgrespoolsize 8;
    rewrite ^/func2/(.*)$ /$1 break;
}
```

两个入口各自独立的函数表与连接池，互不影响；`/_poolctl` 用 `conn=0/1` 分入口管理。

### 2.5 海量数据：日志表是第一对象

`public.serverlog` 每请求写一行（含 params/res 全文），是增长最快、最先需要治理的表；
`public.loginlog` 与审计行次之。三条措施按顺序做：

**① 按月分区（主键已含时间列，DDL 就绪）**

```sql
-- 一次性改造（低峰期执行；分区表无法直接 ALTER，需新建+迁数据）
CREATE TABLE public.serverlog_p (LIKE public.serverlog INCLUDING ALL)
  PARTITION BY RANGE (logtime);
CREATE TABLE public.serverlog_2026_11 PARTITION OF public.serverlog_p
  FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');
-- …按需预建未来 2~3 个月，之后每月提前建下月分区
```

收益：**归档变成 `DROP` 老分区（秒级）**，而不是 `DELETE` 千万行（锁表 + 膨胀）；
查询带 `logtime` 条件时自动分区裁剪。

**② 查询所需索引**

```sql
CREATE INDEX ON public.serverlog (logtime DESC);          -- 时间范围查询/列表默认排序
CREATE INDEX ON public.serverlog (funcname, logtime DESC);
CREATE INDEX ON public.serverlog (operatorid, logtime DESC);
CREATE INDEX ON public.loginlog  (logintime DESC);
CREATE INDEX ON public.loginlog  (accounts, logintime DESC);   -- 登录失败计数（login 热路径）
```

> `login` 函数每次都会按 `accounts + logintime` 统计失败次数，这是登录接口的热查询；
> 数据量大后**这条索引是必需的**，否则登录会随时间线性变慢。

模糊匹配目前用 `position(x in col) > 0`（不做前缀优化），若日志检索成为高频需求，
改用 `pg_trgm` + GIN：

```sql
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX ON public.serverlog USING gin (funcname gin_trgm_ops, params gin_trgm_ops);
```

**③ 归档与留存**

- 归档前先满足等保"日志留存 ≥ 6 个月"；
- 归档做法：`DROP` 超期分区 → 需要留存的先 `\copy` 导出压缩文件再删；
- 每月定期预建下月分区（可用 `pg_cron` 或运维 cron 跑一段建分区 SQL）。

**④ 深分页**

现有列表统一用 `LIMIT/OFFSET`（`offset` 已做上限保护，不会越翻越慢到失控）。
日志类数据在千万级后，深翻页仍会退化；届时应改为 **keyset 分页**
（`WHERE logid < :last_id ORDER BY logid DESC LIMIT :n`），前端传上一页末尾 `logid`。

### 2.6 慢查询治理

当前库**未安装** `pg_stat_statements`（实测 `pg_extension` 中无该扩展）。海量数据上线前建议：

```sql
-- postgresql.conf: shared_preload_libraries = 'pg_stat_statements'
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
```

然后按 `total_exec_time` 排序找出真正的热点函数，逐个 `EXPLAIN (ANALYZE, BUFFERS)`
验证是否走索引；同时监控 `pg_stat_user_tables` 的 `seq_scan`（大表顺序扫描暴涨＝缺索引）。

### 2.7 缓存：先别做

只有当**同一个低频变更数据被高频读取**（菜单树、权限树、错误码表）且 PG 已成为瓶颈时才考虑。
本项目的鉴权每次都查库，这是"默认安全"的代价，也是正确性保证；
**加缓存必须解决"权限变更立即失效"**，否则等于制造越权漏洞。优先级排在索引与分区之后。

### 2.8 演进路线（按投入产出排序）

| 步 | 动作 | 收益 | 成本 |
|---|---|---|---|
| 0 | `worker_processes auto` + `worker_connections` 调大 | 并发处理能力 ×核数 | 改 1 行 |
| 1 | 日志表索引 + 登录失败计数索引 | 消除随时间劣化 | 几行 DDL |
| 2 | 日志按月分区 + 归档 | 海量数据可长期运行 | 一次性 DDL + 月度脚本 |
| 3 | PG 主备 + Patroni/流复制 | 消除数据库单点 | 需额外组件 |
| 4 | nginx 多实例 + VIP/LB | 吞吐随机器数近线性 | 需运维配套 |
| 5 | 读写分离 / 按 schema 拆库 | 读扩展、故障隔离 | 部署改造 |
| 6 | PgBouncer / 缓存 | 仅在连接或热点成为瓶颈时 | 需额外组件 |

**判断该走到哪一步**：先看 `/_poolctl` 的 `fail/drop`、PG 的 `max_connections` 使用率、
`serverlog` 表大小与查询 P95——**哪一项先告警就做对应那一步**，不要提前上复杂度。

### 2.9 高可用（避免单点）

当前默认部署是**单机**：nginx、PG、前端在同一台机器上，任何一处故障都会导致整体不可用。
生产环境按下面的顺序消除单点（**注意：这是部署形态，不是本项目的代码限制**）：

| 层 | 目标形态 | 方案 | 切换时间量级 |
|---|---|---|---|
| 入口 | 无双点 | 多实例 + keepalived VIP（L4）或入口 nginx upstream（L7） | 秒级 |
| 应用 | 可任意增删 | nginx+模块实例无状态，直接加机器 | 无需切换 |
| 数据库 | 无单点 | 流复制 + **Patroni + etcd** 自动选主（或 repmgr） | 10~30 s（异步复制需接受少量数据回退） |
| 连接入口 | 跟随主库 | haproxy / VIP 指向当前主库，模块 conninfo 只配一个地址 | 对模块透明 |
| 备份 | 可回到任意时刻 | 现有 `pg_dump` 每日全备 → 升级 **pgBackRest**（全量+增量+WAL 归档，PITR） | 分钟级恢复 |
| 灾备 | 机房级容灾 | 异地异步复制 standby（同时满足等保"异地备份"） | 分钟级 |

几点必须知道的约束：

1. **实例无状态是水平扩展的前提**——token/权限/审计都在 PG 里，任何实例都能处理任何请求，
   不需要粘性会话；但**这也意味着 PG 是唯一状态源，PG 挂了所有实例一起挂**，
   所以第 3 步（PG 主备）的优先级高于加 nginx 机器。
2. 多实例时每个实例各自持有连接池，PG 总连接数 ≈ **实例数 × worker 数 × `postgrespoolsize`**，
   必须与 `max_connections` 对齐（或统一走 PgBouncer）。
3. `limit_req`/`limit_conn` 是**单机维度**的，多实例后全局配额要放到 LB 层重新评估。
4. 健康检查建议直接探业务接口：`POST /func/sysinfo/help_query` 无 token 返回
   `100005` 即为存活（这同时验证了 nginx、模块、PG 连接三层的可用性）。

## 三、和其他技术栈比，快在哪、慢在哪

| 维度 | 本项目（nginx + PG 函数） | 典型 Tomcat + Spring Boot + ORM |
|---|---|---|
| 请求链路 | 浏览器 → nginx 模块 → PG 函数，**1 跳** | 容器线程 → Filter → Controller → Service → ORM → JDBC → PG |
| 单请求开销 | 实测 **2~4 ms**（含鉴权与审计） | 同等 CRUD 通常数十毫秒量级（ORM 映射、对象分配、序列化叠加） |
| 运行时 | 无 JVM，无预热，常驻内存小 | JVM 需预热与较大堆内存 |
| 事务边界 | **一个函数 = 一个事务**，plpgsql 异常自动回滚 | 依赖 `@Transactional` 传播配置，跨层易出错 |
| 权限默认值 | 函数入口强制 `check_login`，**漏写即不可用（默认安全）** | 依赖拦截器/注解逐个标注，漏配即裸奔 |
| 新模块成本 | 6 个函数 + 1 行页面声明（或生成器一键） | Controller/Service/Repository/DTO/VO 多层样板 |
| 部署面 | 一个 nginx + 一个 PG | 需 JDK + 容器 + 构建产物 + 依赖管理 |
| **不擅长** | 复杂业务编排、大量第三方集成、消息/任务调度、富领域模型、团队需要强类型分层与单元测试体系 | 短链路、极低延迟的数据密集型后台 |

公平说明：上表 Tomcat 侧为**架构分析**与公开经验量级，本机未安装 Java，未做同环境对照实测。
两者定位不同——本项目适合**以数据库为中心的管理系统**（人事、财务、台账、各类业务后台），
不适合把复杂应用逻辑硬塞进存储过程。

## 四、一句话检查清单

上线前对照（**每项都可用本文的命令自查**）：

- [ ] `worker_processes` 是否等于/接近 CPU 核数，`worker_connections` 是否与 `ulimit -n` 匹配
- [ ] `limit_req` 的 `rate` 是否按"正常用户量 × 频率"设定（**别用默认值上线**）
- [ ] `/_poolctl` 的 `hit/get` > 99%、`fail` 与 `drop` 为 0
- [ ] `public.loginlog(accounts, logintime)` 索引已在（登录热路径）
- [ ] `public.serverlog` 有 `logtime` 索引，且已规划按月分区与归档
- [ ] `pg_stat_statements` 已启用（否则出了问题无法定位）
- [ ] 连接账号是最小权限的 `conn`，而非库 owner
- [ ] 已明确"日志留存 ≥ 6 个月"与备份恢复演练负责人


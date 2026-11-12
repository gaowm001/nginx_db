# ngxdb 系统功能测试方案与测试用例

> 版本：v1.0（2026-11-12）　适用：`extension/ngxdb_postgresql.sql`（PostgreSQL 18）+ nginx postgres 模块 + `html/` 前端
> 测试代码：`test/test_ngxdb_db.py`（数据库层）、`test/test_ngxdb_web.py`（网页/接口层）、`test/run_all.sh`（一键执行）

---

## 1. 系统功能分析

### 1.1 架构与数据流

```
浏览器(html/ngxdb)
   │  POST /func/<schema>.<函数名>  (参数名=函数形参去掉 p_ 前缀)
   ▼
nginx  ngx_http_postgres_module  (postgresconn 连接 PostgreSQL)
   │  SELECT <schema>.<函数名>(p_token:=..., ...)
   ▼
PostgreSQL: gm.check_login(令牌鉴权+权限校验) → 业务函数 → gm.returnjson(统一 JSON)
   返回 {"errorcode":0/错误码, "message":"...", "info":{...}}
```

### 1.2 功能模块与函数分布（共 78 个函数）

| 模式 | 数量 | 函数 |
|---|---|---|
| gm | 8 | actionupdate、check_login、gethelp、gettoken、gettokene、http_get、http_post、returnjson |
| public | 1 | _ensure_roles |
| sysinfo | 68 | login、loginaccount、regaccount、aes_encrypt、aes_decrypt、errorcode_query、actions_query、serverlog_query、systeminfo_×6、sysaction_×6、sysorg_×6、orgtype_×6、operinfo_×6、operinfoorg_×6、operinfopermission_×6、roleinfo_×6、province_×6、city_×6 |
| conn / crypto | 0 | 无自定义函数（crypto 仅供 pgcrypto 扩展对象） |

> 每个 CRUD 模块固定 6 个函数：`_add / _edit / _merge / _del / _undel / _query`。

### 1.3 权限模型

- 登录成功返回 `token`（双层 AES：外层用 `actions.actionid=101` 的 params 作 key，内层用子系统 `prikey`）与 `actions`（该用户在本子系统的权限 id 集合，含角色继承展开）。
- 每个业务函数第一步 `gm.check_login(p_token, p_actionid)`：校验令牌→子系统→操作员→单人/多人登录→令牌时效→该 actionid 是否在用户权限树内；不通过返回 100005/100006/100008/100011/100012。
- 权限来源：`operpermission(permissiontype=1 直接权限 / 2 角色)`；角色经 `rolepermission` 递归继承。

### 1.4 主要业务实体

子系统 systeminfo、系统权限树 sysaction（upid/idpath/idlevel 树）、部门树 sysorg、部门类型 orgtype、员工 operinfo、员工部门 operinfoorg、员工权限 operinfopermission、角色 roleinfo、角色权限 rolepermission、省 province、市 city、登录日志 loginlog、服务日志 serverlog、错误码 errorcode、权限名字典 actions、第三方应用参数 appparams、页面帮助 funchtml。

### 1.5 错误码（节选，共 33 条）

| 码 | 含义 | 码 | 含义 |
|---|---|---|---|
| 0 | 成功 | 100001 | 帐号密码错误 |
| 100002 | 登录错误超 5 次，锁定 10 分钟 | 100003 | 登录错误超 10 次，锁定 3 小时 |
| 100005 | 非法登录（无令牌/令牌不可解） | 100006 | 登录已失效（过期/被踢） |
| 100008 | 无此权限 | 100009/100010 | 账号/密码为空 |
| 100011 | 操作员信息出错 | 100012 | 子系统信息出错 |
| 100013 | 系统权限信息出错 | 100014 | 上级编号不存在 |
| 100015 | 顶级不能操作 | 100016 | 有下级不能删除 |
| 100017 | 不能循环定义 | 100018 | 部门信息出错 |
| 100020 | 部门类型信息出错 | 100022 | 账号不能重复 |
| 100023 | 角色信息出错 | 100026-100029 | 注册相关错误 |
| 100033 | 省信息出错 | 100034 | 市信息出错 |

---

## 2. 测试策略

| 层次 | 脚本 | 目标库 | 说明 |
|---|---|---|---|
| 第一层：数据库函数测试 | test_ngxdb_db.py | **ngxdb_test（隔离库，由 run_all.sh 用 dump 重建）** | 直接 SELECT 函数，验证返回 JSON、错误码、数据落库正确性；不污染在用 postgres 库 |
| 第二层：网页/接口测试 | test_ngxdb_web.py | 运行中的 nginx(127.0.0.1:80) + postgres 库 | 静态页、登录、全部查询接口、鉴权失败路径、一次自清理 CRUD 闭环、并发冒烟 |

- 通过标准：全部用例 PASS；KNOWN 类（依赖缺失/数据缺失）不计失败但必须列明。
- DB 层每个用例独立记录 PASS/FAIL 与详情，最后输出统计与失败清单。
- 会话安全类用例（互踢、过期、暴力锁定）放到最后执行，避免污染其他用例的令牌。
- Web 层 CRUD 仅使用名称带 “测试” 的记录，结束后物理删除，保持在用库干净。

---

## 3. 测试场景与用例总表

### 场景 A：基础工具函数（G0）
| 用例 | 函数 | 输入 | 预期 |
|---|---|---|---|
| A1 | gm.returnjson | (0) | errorcode=0，message=执行成功 |
| A2 | gm.returnjson | (100001) | message=帐号密码错误 |
| A3 | gm.returnjson | (99999) | message=未知错误 |
| A4 | gm.actionupdate | () | true，sysaction.idpath 非空 |
| A5 | public._ensure_roles | () | 正常执行（幂等） |
| A6 | aes_encrypt→aes_decrypt | 'ngxdb测试'+key | 往返一致 |
| A7 | sysinfo.aes_decrypt | 正确密文+错误 key | NULL（容错） |
| A8 | gm.gethelp | () | 文本或 NULL（funchtml 空→KNOWN-DATA） |
| A9 | gm.http_get | 本机 login.html | 非空文本 |
| A10 | gm.http_post | 本机地址 | 文本（405 也算连通） |
| A11 | gm.gettoken/gettokene | () | 依赖 pkg_service：报错→KNOWN-DEP；成功→PASS |

### 场景 B：登录与鉴权（G1）
| 用例 | 函数 | 输入 | 预期 |
|---|---|---|---|
| B1 | sysinfo.login | (null,'x') | 100009 |
| B2 | sysinfo.login | ('admin',null) | 100010 |
| B3 | sysinfo.login | ('admin','错密码') | 100001，loginlog+1 |
| B4 | sysinfo.login | ('admin','123456','nosuchsys') | 100001 |
| B5 | sysinfo.login | ('admin','123456') | 0，token/actions 正常 |
| B6 | gm.check_login | (null,150) | 100005 |
| B7 | gm.check_login | ('garbage',150) | 100012（实测行为） |
| B8 | gm.check_login | (有效token,150) | 0 |
| B9 | gm.check_login | (有效token,99999) | 100008 |
| B10 | 权限矩阵 | 仅 106 权限用户 | check(106)=0、check(101)=100008 |

### 场景 C：子系统 systeminfo（G2，权限 101）
C1 add(自动id)→0；C2 add(已存在 id=100)→100012；C3 add(prikey=null)→100012；C4 query total≥2；C5 分页 rows=1/page=1；C6 page 越界→空行不报错；C7 edit 改名→0 且生效；C8 merge→0；C9 del→isused=0；C10 del(999999)→100012；C11 undel→0；C12 无权限用户 add→100008。

### 场景 D：系统权限 sysaction（G3）
D1 add(upid=100)→0 且 idpath=100.xxx/idlevel=2；D2 add(upid=999999)→100014；D3 add(重复 id=100)→100013；D4 query 全量含 idpath/idlevel 树序；D5 query(idpath 过滤)仅子树；D6 edit 改名→0；D7 edit(upid=999999)→100014；D8 merge→0；D9 del(叶子)→0；D10 del(100)→100016；D11 del(999999)→100013；D12 undel→0；D13 undel(100)→100015。

### 场景 E：部门 orgtype / sysorg（G4）
E1 orgtype_add→0；E2 重复 id→100020；E3 query total≥1；E4 edit→0；E5 merge→0；E6 del→0；E7 del(999999)→100020；E8 undel→0；E9 sysorg_add(父=根)→0；E10 upid=999999→100014；E11 重复 id→100018；E12 query 含 idpath/idlevel；E13 二级部门 idlevel=2；E14 edit→0；E15 merge→0；E16 del(叶)→0；E17 del(有下级)→100016；E18 del(顶级100)→100015；E19 undel→0。

### 场景 F：员工 operinfo / operinfoorg / operinfopermission（G5）
F1 operinfo_add(106 权限)→0 返回 id+pass；F2 数据验证 operpermission/sysoper 落库（⚠ 疑 AND 恒假缺陷）；F3 重复 accounts→100022；F4 已存在 operatorid→100011；F5 query 全量/分页/过滤；F6 edit→0；F7 merge→0；F8 del→0；F9 del(999999)→100011；F10 undel→0；F11 新员工用 pass 登录成功且 actions 仅 106 子树；F12 operinfoorg_add→0；F13 edit→0；F14 merge→0；F15 query→0；F16 del→0；F17 undel→0；F18 operinfopermission_add→0+数据验证（⚠ 同 F2）；F19 edit→0；F20 merge→0；F21 query→0；F22 del→0；F23 undel→0。

### 场景 G：角色 roleinfo（G6）
G1 add(106 权限)→0；G2 重复 id→100023；G3 query 含 rolepermissionjson；G4 edit→0；G5 角色引用自身→100017 循环定义；G6 merge→0；G7 del→0；G8 del(999999)→100023；G9 undel→0；G10 角色赋给员工后登录 actions 含 106（继承验证）。

### 场景 H：省市 province / city（G7）
H1 province_add→0；H2 重复 pid→100033；H3 query total≥35；H4 edit→0；H5 merge→0；H6 del→0；H7 del(999999)→100033；H8 undel→0；H9 city_add→0；H10 pid 无效→100033；H11 cid 重复→100034；H12 query 按 pid 过滤；H13 edit→0；H14 edit(pid 无效)→100033；H15 merge→0；H16 del→0；H17 del(999999)→100034；H18 undel→0。

### 场景 I：日志与字典（G8）
I1 serverlog_query→0；I2 errorcode_query 模糊→0 且 total≤33；I3 actions_query→0 total=67。

### 场景 J：注册与第三方登录（G9）
J1 regaccount(未知系统)→100001；J2 regaccount(无手机+无效 appid)→100026；J3 regaccount(手机+错码)→sms 缺失：异常或 100027→KNOWN-DEP；J4 loginaccount(type=999)→100001；J5 loginaccount(type=101,appid 无效)→100001。

### 场景 K：会话安全（G10，最后执行）
K1 单人登录互踢：tokentype=1 用户两次登录，旧 token→100006；K2 令牌过期：tokentime 改过去→100006；K3 暴力锁定：连错 6 次→100002，继续→100003。

### 场景 L：网页/接口层（test_ngxdb_web.py）
L1 静态页 200；L2 登录成功/错密码/缺账号/缺密码；L3 全部查询接口带 token→0 且含 total；L4 无 token→100005、伪 token→100012/100005；L5 省市 CRUD 闭环（自清理）；L6 不存在函数行为记录；L7 并发冒烟（10 线程×3 接口全 0，验证 POST finalize 修复后连接复用正常）。

### 场景 M：接口文档（G11，2026-11-13 新增）
M1 无 token 调 sysinfo/gethelp→100005；M2 伪 token→100012；M3 admin(经根 100 授权)带 token→0 且 total≥100、rows 含 schemaname/funcname/arguments/returntype/description；M4 gm.gethelp 已删除：调用返回 404/函数不存在类错误；M5 前端 #/gethelp 页面加载、模糊搜索（如搜 sysinfo）命中过滤、分页翻页正常。

---

## 4. 函数覆盖矩阵（78/78）

| 函数 | 用例 | 函数 | 用例 | 函数 | 用例 |
|---|---|---|---|---|---|
| gm.actionupdate | A4 | gm.check_login | B6-B10,K1,K2 | gm.gethelp | A8 |
| gm.gettoken | A11 | gm.gettokene | A11 | gm.http_get | A9 |
| gm.http_post | A10 | gm.returnjson | A1-A3 | public._ensure_roles | A5 |
| sysinfo.login | B1-B5 | sysinfo.loginaccount | J4,J5 | sysinfo.regaccount | J1-J3 |
| sysinfo.aes_encrypt | A6 | sysinfo.aes_decrypt | A6,A7 | sysinfo.errorcode_query | I2 |
| sysinfo.actions_query | I3 | sysinfo.serverlog_query | I1 | sysinfo.systeminfo_add | C1-C3 |
| sysinfo.systeminfo_del | C9,C10 | sysinfo.systeminfo_edit | C7 | sysinfo.systeminfo_merge | C8 |
| sysinfo.systeminfo_query | C4-C6 | sysinfo.systeminfo_undel | C11 | sysinfo.sysaction_add | D1-D3 |
| sysinfo.sysaction_del | D9-D11 | sysinfo.sysaction_edit | D6,D7 | sysinfo.sysaction_merge | D8 |
| sysinfo.sysaction_query | D4,D5 | sysinfo.sysaction_undel | D12,D13 | sysinfo.sysorg_add | E9-E11,E13 |
| sysinfo.sysorg_del | E16-E18 | sysinfo.sysorg_edit | E14 | sysinfo.sysorg_merge | E15 |
| sysinfo.sysorg_query | E12 | sysinfo.sysorg_undel | E19 | sysinfo.orgtype_add | E1,E2 |
| sysinfo.orgtype_del | E6,E7 | sysinfo.orgtype_edit | E4 | sysinfo.orgtype_merge | E5 |
| sysinfo.orgtype_query | E3 | sysinfo.orgtype_undel | E8 | sysinfo.operinfo_add | F1-F4 |
| sysinfo.operinfo_del | F8,F9 | sysinfo.operinfo_edit | F6 | sysinfo.operinfo_merge | F7 |
| sysinfo.operinfo_query | F5 | sysinfo.operinfo_undel | F10 | sysinfo.operinfoorg_add | F12 |
| sysinfo.operinfoorg_del | F16 | sysinfo.operinfoorg_edit | F13 | sysinfo.operinfoorg_merge | F14 |
| sysinfo.operinfoorg_query | F15 | sysinfo.operinfoorg_undel | F17 | sysinfo.operinfopermission_add | F18 |
| sysinfo.operinfopermission_del | F22 | sysinfo.operinfopermission_edit | F19 | sysinfo.operinfopermission_merge | F20 |
| sysinfo.operinfopermission_query | F21 | sysinfo.operinfopermission_undel | F23 | sysinfo.province_add | H1,H2 |
| sysinfo.province_del | H6,H7 | sysinfo.province_edit | H4 | sysinfo.province_merge | H5 |
| sysinfo.province_query | H3 | sysinfo.province_undel | H8 | sysinfo.city_add | H9-H11 |
| sysinfo.city_del | H16,H17 | sysinfo.city_edit | H13,H14 | sysinfo.city_merge | H15 |
| sysinfo.city_query | H12 | sysinfo.city_undel | H18 | sysinfo.roleinfo_add | G1,G2 |
| sysinfo.roleinfo_del | G7,G8 | sysinfo.roleinfo_edit | G4,G5 | sysinfo.roleinfo_merge | G6 |
| sysinfo.roleinfo_query | G3 | sysinfo.roleinfo_undel | G9 | — | — |

## 5. 已知限制与预期缺陷（不视为测试失败，但必须列明）

1. **pkg_service 模式缺失**：`gm.gettoken/gettokene` 调用 `pkg_service.job_finish`，dump 未包含该包 → 调用必报“schema 不存在”（A11 记 KNOWN-DEP）。
2. **sms 模式缺失**：`regaccount` 手机验证码路径查询 `sms.sendcode` → 报错（J3 记 KNOWN-DEP）。
3. **funchtml 空表**：`gm.gethelp` 返回 NULL（A8 记 KNOWN-DATA，属种子数据缺失）。
4. **疑似 SQL 缺陷（测试重点 F2/F11/F18）**：`operinfo_add`、`operinfo_edit`、`operinfopermission_add` 的权限 JSON 插入条件为 `(permissiontype=1 and ...) and (permissiontype=2 and ...)` 恒假，可能导致新员工权限/系统归属写入失败、无法登录。若用例 FAIL，将给出修复补丁（改为 OR）。
5. 微信相关成功路径依赖外部服务与 appparams 真实凭据，仅测试到错误分支为止。

## 6. 执行方法

```bash
cd <nginx_db 模块目录>（即本仓库根目录）
bash test/run_all.sh            # 全部：重建 ngxdb_test → DB 测试 → Web 测试 → 汇总
python3 test/test_ngxdb_db.py   # 仅数据库层（需先有 ngxdb_test 库）
python3 test/test_ngxdb_web.py  # 仅网页层（针对运行中的 nginx）
```




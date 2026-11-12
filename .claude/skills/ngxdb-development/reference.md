# NGXDB 开发参考（代码模板与文件地图）

与 SKILL.md 配套使用。以下模板均来自本项目已验证的实现。

## 1. 手工开发 CRUD 骨架（以 hr.emp 为例）

```sql
CREATE SCHEMA IF NOT EXISTS hr;

CREATE TABLE IF NOT EXISTS hr.emp(
  id bigint PRIMARY KEY,
  empno varchar(32) NOT NULL,
  empname varchar(64) NOT NULL,
  deptid bigint,
  hiredate date,
  state smallint DEFAULT 1,          -- 1 在职 0 离职
  isdel smallint DEFAULT 0,          -- 逻辑删除
  updatetime timestamp DEFAULT now()
);
ALTER TABLE hr.emp OWNER TO gm;
COMMENT ON TABLE hr.emp IS '人事-员工档案';

CREATE OR REPLACE FUNCTION hr.emp_query(p_token varchar, p_page int, p_pagesize int, p_key varchar)
RETURNS json LANGUAGE plpgsql SECURITY DEFINER AS $$
declare v_check json; v_rows json; v_total bigint;
begin
  v_check:=gm.check_login(p_token, 9201);                 -- ① 鉴权：权限点见 §2
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  select count(*) into v_total from hr.emp
   where isdel=0 and (p_key is null or empname ilike '%'||p_key||'%' or empno ilike '%'||p_key||'%');
  select array_to_json(array_agg(row_to_json(t))) into v_rows
    from (select * from hr.emp
           where isdel=0 and (p_key is null or empname ilike '%'||p_key||'%' or empno ilike '%'||p_key||'%')
           order by id desc limit coalesce(p_pagesize,20)
           offset coalesce(p_page,1)*coalesce(p_pagesize,20)-coalesce(p_pagesize,20)) t;
  return gm.returnjson(0, json_build_object('total',v_total,'rows',coalesce(v_rows,'[]'::json)));
end $$;
ALTER FUNCTION hr.emp_query(varchar,int,int,varchar) OWNER TO gm;
```

写操作（add/edit/del）同理：入口鉴权 → 业务校验（重复、状态、余额/库存等，返回
对应错误码）→ `INSERT/UPDATE/DELETE` → `gm.returnjson(0)`。
口令类字段直接写明文即可——`sysinfo.hash_pass_trigger` 会自动转为 blowfish 哈希。

## 2. 权限节点模板（挂在根 100 之下）

```sql
INSERT INTO sysinfo.sysaction (isused,idpath,idlevel,idcount,upid,params,systemid,sysactionid,sysactionname,actionid,isdefault)
VALUES (1,'100.9200',2,0,100,NULL,100,9200,'人事管理',NULL,NULL) ON CONFLICT DO NOTHING;

INSERT INTO sysinfo.sysaction (isused,idpath,idlevel,idcount,upid,params,systemid,sysactionid,sysactionname,actionid,isdefault)
VALUES (1,'100.9200.9201',3,0,9200,NULL,100,9201,'员工档案',NULL,NULL),
       (1,'100.9200.9201.9211',4,0,9201,NULL,100,9211,'新增员工',9211,NULL),
       (1,'100.9200.9201.9212',4,0,9201,NULL,100,9212,'修改员工',9212,NULL),
       (1,'100.9200.9201.9213',4,0,9201,NULL,100,9213,'删除员工',9213,NULL)
ON CONFLICT DO NOTHING;
```

- 函数内 `check_login` 的权限点必须与 `actionid` 一致：query 用菜单节点号（9201），
  add/edit/del 分别用 9211/9212/9213。
- 给具体员工授权走"员工权限"页面（operinfo 模块）；admin 经根 100 自动全权。
- 财务、ERP 另起号段（如 93xx、94xx）。**编号一经使用不要复用/改动。**

## 3. 前端声明（modules.js）

在 `html/assets/modules.js` 仿照现有行添加 `P()` 声明（菜单号、权限点、base、列、
表单字段、操作权限），随后 `python3 tools/gen_module.py --def ...` 也可自动生成。
页面默认获得：列表、模糊搜索、分页、弹窗表单、删除/恢复、导出；树形模块自动获得
"资源管理器"两栏布局。特殊交互页面再在 `app.js` 加路由特判（参考 renderGetHelp）。

## 4. 上线部署

```bash
# Linux
python3 tools/gen_module.py --def tools/my.json --apply --deploy
sudo systemctl restart nginx
# Windows（管理员 PowerShell）
powershell -NoProfile -ExecutionPolicy Bypass -File tools\secure_deploy.ps1
```

- 部署目录：Linux `/opt/nginx/html/ngxdb`；Windows `C:\nginx\html\ngxdb`
  （环境变量 `NGXDB_DEPLOY_DIR` 覆盖）。
- 全量回归：`bash test/run_all.sh`（Linux）/ `test/run_all.ps1`（Windows）；
  单层运行：`python3 test/test_ngxdb_db.py --password ...`、
  `python3 test/test_ngxdb_web.py --base https://127.0.0.1`。

## 5. 文件地图

| 路径 | 作用 |
|---|---|
| `extension/ngxdb_postgresql.sql` | 完整库脚本（唯一事实来源，手工变更必须回写） |
| `html/index.html` | 前端入口（资源版本号 `?v=N`，改动后 +1） |
| `html/assets/modules.js` | 模块声明（菜单/权限/列/表单） |
| `html/assets/app.js` | 通用渲染 + 特殊页路由 |
| `html/assets/core.js` | api()/token/登录跳转 |
| `tools/gen_module.py` | 模块生成器（--def/--apply/--apply-tx/--deploy/--print-sql） |
| `tools/examples/*.json` | 生成器示例定义（notice=单表、subtask=父子、region=树） |
| `tools/secure_deploy.sh|.ps1` | 等保部署：TLS/限流/ngxapp 口令轮换/备份任务 |
| `tools/backup.sh|.ps1` | 每日 pg_dump 备份（保留 14 份） |
| `test/run_all.sh|.ps1`、`test/test_ngxdb_{db,web}.py` | 全量回归（先重建隔离测试库 ngxdb_test） |
| `test/TEST_PLAN.md` | 测试场景（A..N）与函数覆盖矩阵（81/81） |
| `README.md` | 架构、原理、二次开发指南、等保整改记录、Windows 支持 |

## 6. 错误码速查（returnjson 的 message 来自 sysinfo.errorcode）

| 码 | 含义 |
|---|---|
| 0 | 成功 |
| 100001 | 帐号密码错误 |
| 100005 | 非法登录（无 token） |
| 100006 | 登录已失效（token 过期） |
| 100008 | 无此权限 |
| 100009/100010 | 账号/密码等参数为空 |
| 100012 | 子系统信息出错（token 解析失败/伪造） |
| 100014/100015/100016/100017 | 上级不存在/顶级不能删/有下级不能删/循环定义（树形） |
| 100018 | 记录不存在 |
| 100035 | 口令复杂度不足 |

新增错误码：先查 `sysinfo.errorcode` 选未占用号（100019/100021 已被占用，勿再用），
INSERT 后即可在 `gm.returnjson(码)` 中使用。

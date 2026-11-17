# NGXDB 本地管理台（admin）

面向本地运维/开发的**独立**小工具：在浏览器里直接管理本地 PostgreSQL 的表，并把任意一张
**单表 / 父子表 / 树表** 一键生成为 NGXDB 业务模块（建表 SQL + 增删改查函数 + 前端模块配置），
支持页面预览与发布到本地 nginx。

* **独立系统**：与 ngxdb 的登录/权限体系无关，本管理台自身不要求登录。
* **仅限本机**：后端只监听 `127.0.0.1:8765`，且校验请求来源必须是本机；前端由后端直接服务，浏览器直连 `http://127.0.0.1:8765/` 打开（不经 nginx）。
* **只动本地**：仅对本地 PostgreSQL（psql）与本地 nginx 静态目录 / `/func/_reloadfunctions` 热重载接口操作。

## 目录

```
admin/
├── app.py            # 后端（Python 标准库，无第三方依赖）：静态页 + JSON API
├── gen_pyfuncs.py    # gm Python 模式（plpython3u）生成器（保留；当前生成入口固定 plpgsql）
├── deploy.sh         # 注册/启动后端服务（页面由后端直接服务，不发布到 nginx；可重复执行）
├── static/           # 前端（index.html / admin.js / admin.css / preview.html）
├── data/             # 运行数据：生成记录、发布前备份、服务日志
└── README.md
```

## 使用

```bash
python3 admin/app.py        # 直接启动（或 bash admin/deploy.sh 一键启动）
# 浏览器打开 http://127.0.0.1:8765/
```

功能：

1. **模式管理**：新建模式（schema，可附中文说明）。创建时自动执行与 `sysinfo`/`gm` 一致的最小授权
   （`GRANT USAGE ON SCHEMA ... TO conn`、`GRANT EXECUTE ON ALL FUNCTIONS ... TO conn`、
   `ALTER DEFAULT PRIVILEGES IN SCHEMA ... GRANT EXECUTE ON FUNCTIONS TO conn`），
   确保之后在该模式下生成的函数发布后可被 nginx（conn 连接账号）直接调用，无权限问题。
2. **数据表管理**：模式可选（下拉选择，也可现场“新建模式”）；新建表**不自动添加**
   `isused/systemid` 结构列（弹窗内提供“＋ 启用列(isused)”、“＋ 系统列(systemid)”快捷按钮，
   由用户决定是否添加）；字段类型覆盖 PostgreSQL 常用类型（白名单 28 种，含别名写法，
   支持 `[]` 数组后缀如 `integer[]`），`varchar/char/numeric`
   的长度/精度可自定义；**树表**自动带 `upid/idpath/idlevel/idcount` 并**内置 id=100 的
   默认根节点**（幂等去重），**父子表**可一次建父表+子表；**修改表与新建表同表单**
   （预填表信息，按差异生成 ADD/ALTER TYPE/COMMENT/DROP 事务，结构与主键列受保护；
   删除列时自动清理该列的外键登记 fk_meta）；**删除表**连带清理生成的函数、权限节点
   （sysaction/actions）、前端模块配置（modules.js）与外键登记。
3. **模块生成**：生成 SQL 时自动为新建表在 `sysinfo.errorcode` 登记**专属出错代码**（已存在则复用、去重，重复发布幂等）：函数体中“记录不存在/主键已存在”不再复用通用码 100018（避免报错信息串台，如显示“部门信息出错！”），而是使用“<表中文名>信息出错！”；选择表并指定**表类型**（单表 / 父子表 / 树表）→ 在**权限树**中选择本功能挂载的
   权限节点（树上展示 sysaction 层级，选中即挂载点，支持任意层级；支持模糊搜索定位、
   节点增删改、同级上移/下移——搜索出多个结果时可在搜索结果间移动；菜单/权限ID 自动分配，
   两者为同一 ID，修改任一 actionid 后其后的 ID 自动顺延 +1）→ 字段勾选 → 一键生成
   `plpgsql` SQL（函数以 plpgsql 实现、SECURITY DEFINER、OWNER gm）+ modules.js 配置
   （操作权限**同步写入 actions 具体权限表**，按 actionurl 判重），附页面预览。
   可指定权限归属**系统**（systemid）。
   **重新生成载入已有权限**：同一张表再次生成/发布时，优先按接口地址反查并复用已发布的
   菜单/权限节点（ID 保持稳定，不重复生成；换了挂载点则节点自动迁移）。
   **父子表**：生成时父表与子表均从**已建的表**中选择（子表关联字段下拉为子表全部列，
   默认选中与父表主键同名的列），生成**独立联动模块**——函数名为
   `父表名_子表名_query/add/edit/merge/del`（共 5 个，**无 undel**），模块 id 为
   `父表_子表`，不生成建表语句；父/子表各自的常规 CRUD 请单独生成。
   **外键字段**：`public.fk_meta` 中登记的外键（主模式/主表/主字段/是否可多选/外键模式/
   外键表/外键字段/主表显示字段/主表模块 id）会在生成时自动处理——函数写入前校验值在
   主表存在（多选拆分逐个校验，失败报 100039），query 联查返回主表显示字段；页面表单
   对该字段提供选择器（多选弹窗点“确定”后**整体替换**已选集合，增/删/清空均按最终
   勾选结果回写），列表追加显示列。
   **注意**：fk_meta 仅记录引用关系元数据，**不会在数据库中创建 FOREIGN KEY 约束**
   （全库无任何外键约束）；引用完整性由生成函数在写入时校验（100039）。
4. **发布**：事务内应用 SQL → 同步 modules.js/index.html 到项目 `html/` 与部署目录（默认 `/opt/nginx/html/ngxdb`）→ 自动调用 `/func/_reloadfunctions` 热重载 nginx 函数缓存 → 生成模块即刻在 ngxdb 前端可用。
5. **业务库自动探测**：nginx postgres 模块实际连接哪个库（postgres/ngxdb），由“模块函数数”比对自动识别；也可用 `config.json`/环境变量 `PGDATABASE` 显式指定。

## 表类型（kind）

| kind | 结构 | 函数行为（plpgsql 与 plpython3u 语义一致） |
|---|---|---|
| `single` 单表 | 普通表 | 标准 CRUD；query 支持文本模糊/数值精确过滤与分页 |
| `parent_child` 父子表 | 新建表可一次建父表+子表（子表含外键列，默认 `upid`）；**生成模块时**父/子表均须为已建表 | 常规模式以父表为主模块：`_query` 每行附 `<子表>json` 全量子明细；`_add/_edit/_merge` 接收 `p_<子表>json` 整表 JSON 按**子表主键**增/删/改（不先删后插，保持子表主键稳定）；`_del` 有子记录时拒绝（100016）。注意：`_edit/_merge` **不传**子 JSON 视为“整表覆盖为空”，与 ngxdb 前端“整表提交”行为配套。**基于已有表生成联动模块**：函数名为 `父表_子表_query/add/edit/merge/del`（5 个，无 undel），模块 id=`父表_子表`，语义同上，不生成建表语句 |
| `tree` 树表 | 带 `upid/idpath/idlevel/idcount`，建表时自动内置 **id=100 默认根节点**（幂等） | `_add/_edit/_merge` 校验上级存在(100014)、禁止循环定义(100017)、支持自定义主键(重复报 100018)，写后全表重算 `idpath/idlevel/idcount`；`_query` 联查 `upname` 并按 `idpath` 排序；`_del` 有下级时拒绝（100016） |

## 权限树操作

权限树展示 `sysinfo.sysaction` 的层级结构（当前账号有权限的节点）：

- **模糊搜索**：按权限名称/ID/idpath 过滤，自动展开命中链并定位到第一个命中节点；
- **新增子节点**：在点选节点下创建（仅写 sysaction 树）；节点 ID 自动分配，并跳过历史
  idpath 中已引用的 ID，避免误挂历史子节点；
- **删除**：**物理删除**点选节点及整个子树（sysaction 与 actions 同事务删除），
  核心节点（100/9001/9002/9003）禁止删除；
- **修改**：表格化编辑——菜单节点名称/启用 + 各操作节点的权限名称/接口地址/说明/启用，
  保存时同一事务更新 sysaction 与 actions 两表（actionid 共用不变）；
- **上移/下移**：搜索出多个结果时，将当前定位的命中节点与搜索结果中的上一个/下一个
  交换树上位置（仅同级节点间，不同级会被拒绝）；无搜索时按真实同级相邻节点移动。

## 系统管理（systeminfo）

- 建模页"系统"下拉的数据源与维护入口：新增系统（名称 + prikey 加密密钥必填，ID 默认
  max+1，prikey 限 16 字符、前端默认自动生成 16 位随机密钥且可修改）、修改名称/启用、
  删除（该系统下仍有权限节点时拒绝）；
- `prikey` 是该子系统登录 token 的 AES 密钥、`algorithm` 为算法（1=AES-128，2=AES-256），
  详见 README「十四、安全与性能验证」；修改会使该系统在线 token 全部失效。

## 字段类型

新建表/加列支持 PostgreSQL 常用类型（与 `tools/gen_module.py` 的 `COL_TYPE_RE` 白名单一致）：

- 文本：`varchar(n)`、`char(n)`、`character(n)`、`text`
- 数值：`smallint`、`integer`、`bigint`、`numeric(p,s)`、`real`、`double precision`、`money`、`serial`、`bigserial`
- 时间：`date`、`time`、`timestamp`、`timestamptz`（即 `timestamp with time zone`）、`interval`
- 其他：`boolean`、`uuid`、`json`、`jsonb`、`bytea`、`inet`、`macaddr`、`cidr`、`xml`
- 以上类型均可加 `[]` 后缀作为数组列（如 `integer[]`，多选外键使用）

`varchar/char/numeric` 的**长度/精度可自定义**（新建表弹窗中类型旁的“长度”输入框，
如 varchar 填 `50`、numeric 填 `10,3`）。生成的函数参数类型与列类型保持一致
（含长度），避免截断/类型不匹配；发布时自动清理同名旧签名函数（避免重载残留）。

## 配置（可选）

`admin/config.json`（或 `config.local.json`，后者优先）：

```json
{"port": 8765, "pg_host": "127.0.0.1", "pg_port": "5432", "pg_db": "postgres",
 "pg_user": "postgres", "pg_password": "***", "deploy_dir": "/opt/nginx/html/ngxdb",
 "reload_url": "https://127.0.0.1/func/_reloadfunctions", "reload_token": ""}
```

环境变量同名覆盖：`ADMIN_PORT/PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD/NGXDB_DEPLOY_DIR/NGXDB_RELOAD_TOKEN`。

## 安全边界

* 后端仅绑定 127.0.0.1，非本机来源请求直接 403；
* 表名/列名强制 `^[a-z][a-z0-9_]*$`，列类型白名单，注释文本转义；
* 文件写入仅允许项目 `html/` 与部署目录两处白名单路径；
* SQL 在事务中执行，失败自动回滚；发布前自动备份 modules.js/index.html 到 `admin/data/backup/<时间戳>/`。

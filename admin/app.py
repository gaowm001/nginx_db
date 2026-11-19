#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""NGXDB 本地管理台（admin）后端服务。

· 独立系统：与 ngxdb 前端/鉴权无关，无需登录；
· 仅限本机：只监听 127.0.0.1，拒绝一切非本机来源；
· 只操作本地 PostgreSQL（psql）与本地 nginx（静态目录 + /func/_reloadfunctions 热重载）。

API 一览（全部 JSON）：
  GET  /api/ping                 存活探测
  GET  /api/info                 数据库/nginx/路径信息
  GET  /api/schemas              业务 schema 列表
  GET  /api/tables?schema=       表清单（行数/大小/注释）
  GET  /api/table?schema=&table= 表结构详情
  GET  /api/ids                  自动推荐 menu/actionid
  GET  /api/published            已生成发布的模块清单
  GET  /api/preview/config      预览页配置
  GET  /api/preview/data        预览页样例数据（只读 SELECT）
  POST /api/table/create        新建表
  POST /api/table/alter         修改表（加列/删列/改类型/改名/注释）
  POST /api/table/drop          删除表（可连带清理生成的函数与权限节点）
  POST /api/gen                 单表模板生成（返回 SQL/modules.js，不落库）
  POST /api/publish             应用 SQL 到库 + 部署前端到 nginx + 热重载
  POST /api/reload              手动触发 nginx 函数缓存热重载
"""
import json
import os
import re
import shutil
import ssl
import subprocess
import sys
import tempfile
import traceback
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BASE = os.path.dirname(os.path.abspath(__file__))
PROJ = os.path.dirname(BASE)
DATA = os.path.join(BASE, 'data')
STATIC = os.path.join(BASE, 'static')
sys.path.insert(0, os.path.join(PROJ, 'tools'))
import gen_module    # noqa: E402  复用单表模板生成器（plpgsql 路径）
import gen_pyfuncs   # noqa: E402  gm Python 模式（plpython3u）生成器

CFG = {
    'host': '127.0.0.1',
    'port': 8765,
    'pg_host': '127.0.0.1',
    'pg_port': '5432',
    'pg_db': 'postgres',
    'pg_user': 'postgres',
    'pg_password': os.environ.get('PGPASSWORD', ''),
    'deploy_dir': '/opt/nginx/html/ngxdb',
    'reload_url': 'https://127.0.0.1/func/_reloadfunctions',
    'reload_token': '',
}
PG_DB_EXPLICIT = False  # config/env 是否显式指定了业务库


def _load_cfg():
    global PG_DB_EXPLICIT
    for name in ('config.json', 'config.local.json'):
        p = os.path.join(BASE, name)
        if os.path.exists(p):
            try:
                CFG.update(json.load(open(p, encoding='utf-8')))
            except Exception as e:  # noqa: BLE001
                print('配置文件 %s 解析失败: %s' % (name, e))
    envmap = {'ADMIN_HOST': 'host', 'ADMIN_PORT': 'port', 'PGHOST': 'pg_host',
              'PGPORT': 'pg_port', 'PGDATABASE': 'pg_db', 'PGUSER': 'pg_user',
              'PGPASSWORD': 'pg_password', 'NGXDB_DEPLOY_DIR': 'deploy_dir',
              'NGXDB_RELOAD_URL': 'reload_url', 'NGXDB_RELOAD_TOKEN': 'reload_token'}
    for k, v in envmap.items():
        if os.environ.get(k):
            CFG[v] = os.environ[k]
    CFG['port'] = int(CFG['port'])
    PG_DB_EXPLICIT = bool(os.environ.get('PGDATABASE')) or \
        any('pg_db' in json.load(open(os.path.join(BASE, n), encoding='utf-8'))
            for n in ('config.json', 'config.local.json') if os.path.exists(os.path.join(BASE, n)))


_load_cfg()


def detect_pg_db():
    """探测 nginx postgres 模块实际连接的业务库：
    对比 /func/_reloadfunctions 报告的函数数与各候选库的过滤函数数，一致者即业务库。
    显式配置（config/env）时不探测。"""
    if PG_DB_EXPLICIT:
        return {'db': CFG['pg_db'], 'detected': False, 'basis': 'explicit config'}
    candidates = ['postgres', 'ngxdb']
    try:
        ctx = ssl._create_unverified_context()
        tok = ('?token=' + CFG['reload_token']) if CFG['reload_token'] else ''
        req = urllib.request.Request(CFG['reload_url'] + tok, data=b'', method='POST')
        body = urllib.request.urlopen(req, timeout=6, context=ctx).read().decode('utf-8', 'replace')
        n = int(re.search(r'"functions"\s*:\s*(\d+)', body).group(1))
    except Exception as e:  # noqa: BLE001
        return {'db': CFG['pg_db'], 'detected': False, 'basis': 'reload 不可达(%s)，使用默认库' % e}
    for db in candidates:
        try:
            cnt = qval("select count(*) from pg_proc t1 left join pg_namespace t2 on t1.pronamespace=t2.oid "
                       "where t2.nspname not in ('pg_catalog','information_schema','private','cron','myself','crypto','gm')", db)
            if int(cnt) == n:
                CFG['pg_db'] = db
                return {'db': db, 'detected': True, 'basis': '函数数 %d 与 nginx 模块一致' % n}
        except Exception:  # noqa: BLE001
            continue
    return {'db': CFG['pg_db'], 'detected': False, 'basis': '未匹配到函数数 %d，使用默认库' % n}


IDENT = re.compile(r'^[a-z][a-z0-9_]{0,62}$')
TYPES = re.compile(r'^(varchar\(\d+\)|character varying\(\d+\)|text|integer|bigint|smallint|'
                   r'numeric\(\d+(,\d+)?\)|boolean|date|timestamp|timestamptz)$')
GEN_TYPES = gen_module.COL_TYPE_RE  # 建表/改列类型白名单（与模板生成器共用一份）
VERBS = {'add': '新增', 'edit': '修改', 'merge': '合并', 'del': '删除', 'undel': '恢复', 'query': '查询'}
LOCK = json  # 占位避免误用；实际用 GIL+文件原子写即可
LAST_GEN = {}


def ident(v):
    v = str(v or '').strip().lower()
    if not IDENT.match(v):
        raise ValueError('非法标识符: %r（仅允许小写字母开头的字母数字下划线）' % v)
    return v


def typ(v):
    v = gen_module.norm_type(v)
    if not GEN_TYPES.match(v):
        raise ValueError('不支持的列类型: %r' % v)
    if v.endswith('[]') and v[:-2] in ('serial', 'bigserial'):
        raise ValueError('不支持的列类型: %r（serial 不支持数组）' % v)
    return v


def _alter_using(col, old, new):
    """列类型变更的 USING 表达式：支持 标量→数组、数组→标量、数组→数组、标量→标量。
       多选外键列（数组）由 varchar 逗号文本安全转换；空串/空数组转空数组。"""
    old_a, new_a = old.endswith('[]'), new.endswith('[]')
    if old_a and new_a:                      # 数组→数组：经 text 解析为新元素类型数组
        oet = old[:-2] or 'text'
        return ("(SELECT coalesce(array_agg(x::{net} ORDER BY ord), ARRAY[]::{net}[]) FROM "
                "unnest((%s)::%s[]) WITH ORDINALITY AS u(x, ord))" % (col, oet, new[:-2]))
    if old_a:                                # 数组→标量：取第一个元素
        return ("COALESCE(NULLIF(split_part(trim(%s::text, '{}'), ',', 1), '')::%s, "
                "NULLIF(split_part(trim(%s::text, '{}'), ',', 2), '')::%s)" % (col, new, col, new))
    if new_a:                                # 标量→数组：逗号拆分逐元素转换（空串→空数组）
        et = new[:-2]
        return ("(SELECT coalesce(array_agg(x::%s ORDER BY ord), ARRAY[]::%s[]) FROM "
                "unnest(string_to_array(%s::text, ',')) WITH ORDINALITY AS u(x, ord) "
                "WHERE trim(x) <> '')" % (et, et, col))
    return '%s::text::%s' % (col, new)       # 标量→标量：经 text 中转


def lit(v):
    """SQL 字符串字面量转义（仅用于 COMMENT 等文本）"""
    return "'" + str(v or '').replace("'", "''") + "'"


def psql(script, db=None):
    """执行 SQL 脚本，返回 (rc, stdout, stderr)。临时文件唯一命名，避免并发覆盖。"""
    fd, tmp = tempfile.mkstemp(prefix='ngxdb_admin_', suffix='.sql')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            f.write(script)
        env = dict(os.environ, PGPASSWORD=CFG['pg_password'])
        cmd = ['psql', '-X', '-q', '-tA', '-v', 'ON_ERROR_STOP=1',
               '-h', str(CFG['pg_host']), '-p', str(CFG['pg_port']),
               '-U', CFG['pg_user'], '-d', db or CFG['pg_db'], '-f', tmp]
        r = subprocess.run(cmd, env=env, capture_output=True, text=True,
                           encoding='utf-8', errors='replace', timeout=60)
        return r.returncode, r.stdout, r.stderr
    finally:
        try:
            os.unlink(tmp)
        except OSError:
            pass



def qjson(sql, db=None):
    """只读查询，返回 list[dict]"""
    wrap = "select coalesce(json_agg(t),'[]'::json)::text from (%s) t" % sql
    rc, out, err = psql(wrap, db)
    if rc != 0:
        raise RuntimeError((err or out).strip()[-500:] or 'SQL 执行失败')
    return json.loads(out.strip() or '[]')


def qval(sql, db=None):
    rc, out, err = psql(sql, db)
    if rc != 0:
        raise RuntimeError((err or out).strip()[-500:] or 'SQL 执行失败')
    return out.strip()


def write_file(path, content):
    """仅允许写入项目 html 与 deploy_dir 两处（发布目标白名单）"""
    real = os.path.realpath(path)
    allowed = [os.path.realpath(os.path.join(PROJ, 'html')),
               os.path.realpath(CFG['deploy_dir']), DATA]
    for root in allowed:
        if real.startswith(root + os.sep):
            os.makedirs(os.path.dirname(real), exist_ok=True)
            with open(real, 'w', encoding='utf-8') as f:
                f.write(content)
            return
    raise PermissionError('拒绝写入: ' + path)


# ----------------------------- 表信息与 DDL -----------------------------

def table_exists(schema, table):
    v = qval("select count(*) from information_schema.tables "
             "where table_schema='%s' and table_name='%s'" % (schema, table))
    return v == '1'


def table_columns(schema, table):
    rows = qjson(
        "select c.column_name as name, c.data_type as data_type, c.udt_name as udt_name, c.character_maximum_length as len, "
        "c.numeric_precision as prec, c.numeric_scale as scale, c.is_nullable as nullable, "
        "c.column_default as default_, c.ordinal_position as pos, "
        "coalesce(col_description((quote_ident(c.table_schema)||'.'||quote_ident(c.table_name))::regclass, c.ordinal_position),'') as comment "
        "from information_schema.columns c "
        "where c.table_schema='%s' and c.table_name='%s' order by c.ordinal_position" % (schema, table))
    for c in rows:  # 统一列类型（数组列 data_type='ARRAY'，按 udt_name 还原为 integer[] 等）
        c['ptype'] = _ptype_of(c)
    return rows


def table_pk(schema, table):
    rows = qjson(
        "select a.attname as name from pg_index i "
        "join pg_class c on c.oid=i.indrelid join pg_namespace n on n.oid=c.relnamespace "
        "join pg_attribute a on a.attrelid=c.oid and a.attnum=any(i.indkey) "
        "where n.nspname='%s' and c.relname='%s' and i.indisprimary" % (schema, table))
    return rows[0]['name'] if rows else None


def table_rows_size(schema, table):
    rows = qjson("select (select count(*) from %s.%s) as rows, "
                 "pg_size_pretty(pg_total_relation_size('%s.%s')) as size"
                 % (schema, table, schema, table))
    return rows[0] if rows else {'rows': 0, 'size': '-'}


def list_tables(schema=None):
    where = "and t.table_schema='%s'" % schema if schema else ''
    return qjson(
        "select t.table_schema as schema, t.table_name as table, "
        "coalesce(obj_description((quote_ident(t.table_schema)||'.'||quote_ident(t.table_name))::regclass),'') as comment, "
        "pg_size_pretty(pg_total_relation_size((quote_ident(t.table_schema)||'.'||quote_ident(t.table_name))::regclass)) as size "
        "from information_schema.tables t "
        "where t.table_type='BASE TABLE' and t.table_schema not in "
        "('pg_catalog','information_schema','pg_toast','cron','private','myself','crypto')" + where +
        " order by t.table_schema, t.table_name")


def create_schema(body):
    """新建模式（schema），并做与 sysinfo/gm 一致的最小授权，
    确保之后在该模式下发布的函数可被 nginx postgres 模块（conn 账号）调用。"""
    schema = ident(body.get('schema'))
    if schema.startswith('pg_') or schema in ('information_schema', 'cron', 'private', 'myself', 'crypto'):
        raise ValueError('该名称为系统/内部模式，禁止使用: ' + schema)
    chinese = str(body.get('chinese') or '').strip()[:60]
    if qval("select count(*) from pg_namespace where nspname='%s'" % schema) == '1':
        raise ValueError('模式 %s 已存在' % schema)
    ddl = ['-- admin 新建模式：%s（%s）' % (schema, chinese or schema),
           'CREATE SCHEMA %s AUTHORIZATION gm;' % schema]
    if chinese:
        ddl.append('COMMENT ON SCHEMA %s IS %s;' % (schema, lit(chinese)))
    ddl += ['-- 与 sysinfo/gm 一致的最小授权：保证新模式下发布的函数可被 conn 调用',
            'GRANT USAGE ON SCHEMA %s TO conn;' % schema,
            'GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA %s TO conn;' % schema,
            'ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT EXECUTE ON FUNCTIONS TO conn;' % schema]
    rc, out, err = psql('\n'.join(ddl) + '\n')
    if rc != 0:
        raise RuntimeError('创建模式失败: ' + (err or out).strip()[-500:])
    return {'ok': True, 'schema': schema, 'ddl': '\n'.join(ddl)}


def _ident_opt(v):
    """可选标识符清洗：空值返回 ''；非空必须合法，否则抛错"""
    v = str(v or '').strip().lower()
    if v and not IDENT.match(v):
        raise ValueError('非法标识符: %r（仅允许小写字母开头的字母数字下划线）' % v)
    return v


def _fk_insert_sql(schema, table, fks):
    """生成 fk_meta 的写入语句（先清空本表配置再插入）；并做标识符/完整性清洗。
       master_column 缺省自动取主表主键；主表必须已存在。"""
    stmts = ["DELETE FROM public.fk_meta WHERE fk_schema='%s' AND fk_table='%s';" % (schema, table)]
    n = 0
    for fk in fks or []:
        fkcol = _ident_opt(fk.get('fk_column') or fk.get('column'))
        ms = _ident_opt(fk.get('master_schema')) or schema
        mt = _ident_opt(fk.get('master_table'))
        mc = _ident_opt(fk.get('master_column'))
        if not mt or not fkcol:
            raise ValueError('外键配置不完整（需要外键字段、主表）: %s' % fk)
        if not mc:
            mc = table_pk(ms, mt) or ''
        if not mc:
            raise ValueError('主表 %s.%s 不存在或无主键，无法确定主字段' % (ms, mt))
        if ms == schema and mt == table:
            raise ValueError('外键主表不能是本表')
        multi = 1 if fk.get('master_multi') in (1, '1', True, 'true') else 0
        disp = str(fk.get('display_columns') or '').strip().replace("'", "''")
        mod = str(fk.get('master_module') or '').strip().replace("'", "''")
        stmts.append("DELETE FROM public.fk_meta WHERE fk_schema='%s' AND fk_table='%s' AND fk_column='%s';"
                     % (schema, table, fkcol))
        stmts.append("INSERT INTO public.fk_meta(master_schema,master_table,master_column,master_multi,"
                     "fk_schema,fk_table,fk_column,display_columns,master_module,isused) "
                     "VALUES ('%s','%s','%s',%d,'%s','%s','%s','%s','%s',1);"
                     % (ms, mt, mc, multi, schema, table, fkcol, disp, mod))
        n += 1
    return stmts, n


def _ptype_of(c):
    """列的规范类型：数组列（data_type=ARRAY）按 udt_name 还原为 base[]；varchar/numeric 保留精度"""
    if c.get('data_type') == 'ARRAY':
        return gen_module.pg_type((c.get('udt_name') or '').lstrip('_')) + '[]'
    if c.get('data_type') == 'character varying' and c.get('len'):
        return 'varchar(%d)' % c['len']
    if c.get('data_type') == 'numeric' and c.get('prec'):
        return 'numeric(%s,%s)' % (c['prec'], c['scale'])
    return c.get('data_type') or 'text'


def save_fks(body):
    """保存表的外键引用配置（全量替换本表配置，事务执行）"""
    schema = ident(body.get('schema') or 'sysinfo')
    table = ident(body.get('table') or '')
    stmts, n = _fk_insert_sql(schema, table, body.get('fks') or [])
    rc, out, err = psql('BEGIN;\n' + '\n'.join(stmts) + '\nCOMMIT;')
    if rc != 0:
        raise RuntimeError('外键配置保存失败: ' + (err or out).strip()[-500:])
    return {'ok': True, 'table': table, 'count': n}


def list_fks(schema, table):
    return qjson("select id,master_schema,master_table,master_column,master_multi,"
                 "fk_schema,fk_table,fk_column,display_columns,master_module,master_db from public.fk_meta "
                 "where isused=1 and fk_schema='%s' and fk_table='%s' order by id" % (schema, table))


FDW_SCHEMA = 'fdwext'      # 外部表映射到本地的 schema（约定名）
FDW_SERVER = 'fkserver'    # postgres_fdw server 名（约定名）


def fkfdw_setup(body):
    """分库外键：建立 postgres_fdw 外部数据访问（server/user mapping/外部表映射）。
       对 fk_meta 中 master_db=dbname 的主表，把远程 schema 的表映射到本地 fdwext schema。"""
    host = str(body.get('host') or '127.0.0.1').strip()
    port = str(body.get('port') or '5432').strip()
    dbname = _ident_opt(body.get('dbname'))
    user = str(body.get('user') or '').strip()
    password = str(body.get('password') or '')
    if not (host and port and dbname and user):
        raise ValueError('请填写外部库连接信息：host/port/dbname/user')
    # 1) 扩展与 server / user mapping（先 DROP CASCADE 再重建，幂等；值转义后内插）
    esc = lambda v: str(v).replace("'", "''")
    stmts = [
        'CREATE EXTENSION IF NOT EXISTS postgres_fdw;',
        'DROP SERVER IF EXISTS %s CASCADE;' % FDW_SERVER,
        "CREATE SERVER %s FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host '%s', port '%s', dbname '%s');"
        % (FDW_SERVER, esc(host), esc(port), esc(dbname)),
        "CREATE USER MAPPING FOR postgres SERVER %s OPTIONS (user '%s', password '%s');"
        % (FDW_SERVER, esc(user), esc(password)),
        "CREATE USER MAPPING FOR gm SERVER %s OPTIONS (user '%s', password '%s');"
        % (FDW_SERVER, esc(user), esc(password)),
    ]
    # 2) 需要映射的远程主表（fk_meta 中 master_db=dbname 的主表，按 远程schema+表 去重）
    mtabs = qjson("select distinct master_schema,master_table from public.fk_meta "
                  "where isused=1 and master_db='%s' order by master_schema,master_table"
                  % dbname.replace("'", "''"))
    for mt in mtabs:
        msch, mtab = mt['master_schema'], mt['master_table']
        stmts += [
            'DROP FOREIGN TABLE IF EXISTS %s.%s;' % (FDW_SCHEMA, mtab),
            'IMPORT FOREIGN SCHEMA %s LIMIT TO (%s) FROM SERVER %s INTO %s;'
            % (msch, mtab, FDW_SERVER, FDW_SCHEMA),
        ]
    stmts += [
        'CREATE SCHEMA IF NOT EXISTS %s;' % FDW_SCHEMA,
        'GRANT USAGE ON SCHEMA %s TO gm;' % FDW_SCHEMA,
        'GRANT SELECT ON ALL TABLES IN SCHEMA %s TO gm;' % FDW_SCHEMA,
    ]
    rc, out, err = psql('BEGIN;\n' + '\n'.join(x for x in stmts if x) + '\nCOMMIT;')
    if rc != 0:
        raise RuntimeError('FDW 建立失败（已回滚）: ' + (err or out).strip()[-800:])
    return {'ok': True, 'server': FDW_SERVER, 'schema': FDW_SCHEMA,
            'tables': [m['master_schema'] + '.' + m['master_table'] for m in mtabs]}


def _dump_replace_funcs(dump, s, t, old, new):
    """在 dump 中仅对 schema.t_ 前缀函数的段落（CREATE FUNCTION 行至 ALTER FUNCTION 行）
       做 old→new 替换，避免误伤 dump 中其它同名引用"""
    out = []
    in_func = False
    head = re.compile(r'CREATE (?:OR REPLACE )?FUNCTION ' + re.escape(s) + r'\.' + re.escape(t) + r'_')
    for ln in dump.split('\n'):
        if not in_func and head.match(ln):
            in_func = True
        elif in_func and ln.startswith('ALTER FUNCTION'):
            in_func = False
        if in_func and old in ln:
            ln = ln.replace(old, new)
        out.append(ln)
    return '\n'.join(out)


def fkfdw_rewrite(body=None):
    """分库外键：批量改写已发布函数——把函数体内对远程主表的本地引用
       （主模式.主表）替换为 fdwext 外部表引用，并同步回写 extension dump。"""
    fks = qjson("select fk_schema,fk_table,master_schema,master_table,master_db from public.fk_meta "
                "where isused=1 and master_db<>'local' order by fk_schema,fk_table")
    changed, skipped = [], []
    dump_path = os.path.join(PROJ, 'extension', 'ngxdb_postgresql.sql')
    dump = open(dump_path, encoding='utf-8').read() if os.path.exists(dump_path) else None
    for fk in fks:
        s, t = fk['fk_schema'], fk['fk_table']
        old = fk['master_schema'] + '.' + fk['master_table']
        new = FDW_SCHEMA + '.' + fk['master_table']
        procs = qjson("select p.oid::bigint as oid from pg_proc p "
                      "join pg_namespace n on n.oid=p.pronamespace "
                      "where n.nspname='%s' and p.proname like '%s\\_%%' escape '\\'" % (s, t))
        n = 0
        for p in procs:
            defdef = qval('select pg_get_functiondef(%s)' % p['oid'])
            if old not in defdef:
                continue
            rc, _, err = psql(defdef.replace(old, new) + ';')
            if rc == 0:
                n += 1
            else:
                raise RuntimeError('改写函数失败: ' + err.strip()[-300:])
        if n:
            changed.append('%s.%s（%d 个函数：%s → %s）' % (s, t, n, old, new))
            if dump is not None:
                dump = _dump_replace_funcs(dump, s, t, old, new)
        else:
            skipped.append('%s.%s（未找到引用 %s 的函数）' % (s, t, old))
    if dump is not None and changed:
        open(dump_path, 'w', encoding='utf-8').write(dump)
    return {'ok': True, 'changed': changed, 'skipped': skipped,
            'hint': '已改写函数并同步 extension/ngxdb_postgresql.sql；请执行 /func/_reloadfunctions 热重载'}


def create_table(body):
    schema = ident(body.get('schema') or 'sysinfo')
    if qval("select count(*) from pg_namespace where nspname='%s'" % schema) != '1':
        raise ValueError('模式 %s 不存在，请先“新建模式”' % schema)
    table = ident(body.get('table'))
    pk = ident(body.get('pk') or (table + 'id'))
    chinese = str(body.get('chinese') or table).strip()[:60]
    kind = body.get('kind') or 'single'
    if kind not in ('single', 'parent_child', 'tree'):
        raise ValueError('kind 必须是 single/parent_child/tree')
    fields = body.get('fields') or []
    if table_exists(schema, table):
        raise ValueError('表 %s.%s 已存在' % (schema, table))
    struct = ('upid', 'idpath', 'idlevel', 'idcount') if kind == 'tree' else ()
    cols = ['  %s bigserial primary key' % pk]
    if kind == 'tree':
        cols += ['  upid bigint default 0', '  idpath character varying(255)',
                 '  idlevel integer', '  idcount integer']
    if body.get('isused_col'):
        cols.append('  isused smallint default 1')
    if body.get('sysid_col'):
        cols.append('  systemid integer')
    for f in fields:
        name = ident(f.get('name'))
        if name == pk:
            raise ValueError('字段名 %s 与主键同名（主键默认为“表名+id”，可在“主键名”中另行指定），请修改字段名' % name)
        if name in ('isused', 'systemid') + struct:
            raise ValueError('字段名 %s 与系统结构列同名，请修改字段名' % name)
        cols.append('  %s %s' % (name, typ(f.get('type') or 'text')))
    ddl = ['-- ======== admin 新建表：%s（%s，%s） ========' % (table, chinese, kind),
           'CREATE TABLE IF NOT EXISTS %s.%s (\n%s\n);' % (schema, table, ',\n'.join(cols)),
           'ALTER TABLE %s.%s OWNER TO gm;' % (schema, table),
           'COMMENT ON TABLE %s.%s IS %s;' % (schema, table, lit(chinese)),
           'COMMENT ON COLUMN %s.%s.%s IS %s;' % (schema, table, pk, lit('主键'))]
    for f in fields:
        ddl.append('COMMENT ON COLUMN %s.%s.%s IS %s;' % (schema, table, ident(f.get('name')), lit(f.get('cn') or '')))
    if kind == 'tree':
        # 树表默认根节点：id=100、upid=0、idpath='100'、idlevel=1、idcount=0（幂等去重）
        rc, rv = [pk, 'upid', 'idpath', 'idlevel', 'idcount'], ['100', '0', "'100'", '1', '0']
        if body.get('isused_col'):
            rc.append('isused'); rv.append('1')
        if body.get('sysid_col'):
            rc.append('systemid'); rv.append('100')
        ddl.append('-- 默认根节点（id=100，幂等去重）')
        ddl.append('INSERT INTO %s.%s (%s)\nSELECT %s\nWHERE NOT EXISTS (SELECT 1 FROM %s.%s WHERE %s=100);'
                   % (schema, table, ','.join(rc), ','.join(rv), schema, table, pk))
    if kind == 'parent_child' and body.get('child_table'):
        ct = ident(body.get('child_table'))
        cpk = ident(body.get('child_pk') or (ct + 'id'))
        cfk = ident(body.get('child_fk') or 'upid')
        ccn = str(body.get('child_chinese') or ct).strip()[:60]
        cfields = body.get('child_fields') or []
        if ct == table or table_exists(schema, ct):
            raise ValueError('子表 %s.%s 已存在或与父表同名' % (schema, ct))
        ccols = ['  %s bigserial primary key' % cpk, '  %s bigint default 0' % cfk]
        for f in cfields:
            name = ident(f.get('name'))
            if name in (cpk, cfk, 'isused', 'systemid'):
                raise ValueError('子表字段名与结构列冲突: ' + name)
            ccols.append('  %s %s' % (name, typ(f.get('type') or 'text')))
        if body.get('isused_col'):
            ccols.append('  isused smallint default 1')
        if body.get('sysid_col'):
            ccols.append('  systemid integer')
        ddl += ['-- admin 新建子表：%s（%s）' % (ct, ccn),
                'CREATE TABLE IF NOT EXISTS %s.%s (\n%s\n);' % (schema, ct, ',\n'.join(ccols)),
                'ALTER TABLE %s.%s OWNER TO gm;' % (schema, ct),
                'COMMENT ON TABLE %s.%s IS %s;' % (schema, ct, lit(ccn)),
                'COMMENT ON COLUMN %s.%s.%s IS %s;' % (schema, ct, cfk, lit('父表主键'))]
        for f in cfields:
            ddl.append('COMMENT ON COLUMN %s.%s.%s IS %s;' % (schema, ct, ident(f.get('name')), lit(f.get('cn') or '')))
    try:
        fk_stmts, fk_n = _fk_insert_sql(schema, table, body.get('fks') or [])
        if fk_n:
            ddl += ['-- 外键引用配置（public.fk_meta）'] + fk_stmts
    except Exception as e:
        raise ValueError('外键配置错误: ' + str(e))
    rc, out, err = psql('\n'.join(ddl) + '\n')
    if rc != 0:
        raise RuntimeError('建表失败: ' + (err or out).strip()[-500:])
    return {'ok': True, 'table': table, 'schema': schema, 'kind': kind, 'fks': fk_n, 'ddl': '\n'.join(ddl)}



def table_comment(schema, table):
    return qval("select coalesce(obj_description((quote_ident('%s')||'.'||quote_ident('%s'))::regclass),'') "
                % (schema, table))


def update_fields(body):
    """修改表结构：按提交的字段行与现有列 diff，生成 ADD/ALTER/COMMENT/DROP 事务
    （结构与主键列不可改删；提交中不存在的业务列将被删除）"""
    schema = ident(body.get('schema') or 'sysinfo')
    table = ident(body.get('table'))
    if not table_exists(schema, table):
        raise ValueError('表 %s.%s 不存在' % (schema, table))
    pk = table_pk(schema, table) or ''
    cols = {c['name']: c for c in table_columns(schema, table)}
    struct = {pk, 'isused', 'systemid', 'upid', 'idpath', 'idlevel', 'idcount'}
    fields = body.get('fields') or []
    steps = []
    seen = set()
    for f in fields:
        name = ident(f.get('name'))
        if name in struct:
            continue                              # 结构列/主键：不可改删
        seen.add(name)
        ctype = typ(f.get('type') or 'text')
        cn = str(f.get('cn') or '').strip()
        if name not in cols:                      # 新增列
            steps.append('ALTER TABLE %s.%s ADD COLUMN %s %s;' % (schema, table, name, ctype))
            if cn:
                steps.append('COMMENT ON COLUMN %s.%s.%s IS %s;' % (schema, table, name, lit(cn)))
        else:                                     # 已有列：类型/注释变更
            cur = cols[name]
            cur_type = (cur['data_type'] or '').lower()
            if cur['data_type'] == 'character varying' and cur.get('len'):
                cur_type = 'varchar(%d)' % cur['len']
            elif cur['data_type'] == 'numeric' and cur.get('prec'):
                cur_type = 'numeric(%s,%s)' % (cur['prec'], cur['scale'])
            if cur_type.replace(' ', '') != ctype.replace(' ', ''):
                steps.append('ALTER TABLE %s.%s ALTER COLUMN %s TYPE %s USING %s;'
                             % (schema, table, name, ctype, _alter_using(name, cur_type, ctype)))
            if cn and cn != (cur.get('comment') or ''):
                steps.append('COMMENT ON COLUMN %s.%s.%s IS %s;' % (schema, table, name, lit(cn)))
    dropped = [cname for cname in cols if cname not in struct and cname not in seen]
    for cname in dropped:                         # 提交中不存在的业务列 → 删除
        steps.append('ALTER TABLE %s.%s DROP COLUMN %s;' % (schema, table, cname))
    if not steps:
        return {'ok': True, 'changed': 0, 'sql': ''}
    rc, out, err = psql('BEGIN;\n' + '\n'.join(steps) + '\nCOMMIT;')
    if rc != 0:
        raise RuntimeError('修改失败（事务已回滚）: ' + (err or out).strip()[-800:])
    if dropped:
        # 删列后同步清理该列的外键登记（fk_meta 残留会导致下次生成函数引用已删列）
        try:
            inlist = ','.join("'%s'" % c for c in dropped)
            psql("DELETE FROM public.fk_meta WHERE fk_schema='%s' AND fk_table='%s' AND fk_column IN (%s);"
                 % (schema, table, inlist))
        except Exception:
            pass
    return {'ok': True, 'changed': len(steps), 'sql': '\n'.join(steps)}


def alter_table(body):
    schema, table = ident(body.get('schema') or 'sysinfo'), ident(body.get('table'))
    op = body.get('op')
    pre = 'ALTER TABLE %s.%s' % (schema, table)
    if op == 'addcol':
        name, ctype = ident(body.get('name')), typ(body.get('type') or 'text')
        sql = '%s ADD COLUMN %s %s;' % (pre, name, ctype)
        if body.get('cn'):
            sql += "\nCOMMENT ON COLUMN %s.%s.%s IS %s;" % (schema, table, name, lit(body['cn']))
    elif op == 'dropcol':
        name = ident(body.get('name'))
        sql = '%s DROP COLUMN IF EXISTS %s;' % (pre, name)
    elif op == 'altertype':
        name, ctype = ident(body.get('name')), typ(body.get('type'))
        cur = {c['name']: c for c in table_columns(schema, table)}
        cur_type = (cur.get(name, {}) or {}).get('data_type', 'text').lower()
        if cur[name].get('len') and cur_type == 'character varying':
            cur_type = 'varchar(%d)' % cur[name]['len']
        sql = '%s ALTER COLUMN %s TYPE %s USING %s;' % (pre, name, ctype, _alter_using(name, cur_type, ctype))
    elif op == 'renamecol':
        old, new = ident(body.get('name')), ident(body.get('new'))
        sql = '%s RENAME COLUMN %s TO %s;' % (pre, old, new)
    elif op == 'renameto':
        sql = '%s RENAME TO %s;' % (pre, ident(body.get('new')))
    elif op == 'comment':
        sql = 'COMMENT ON TABLE %s.%s IS %s;' % (schema, table, lit(body.get('comment')))
    else:
        raise ValueError('不支持的 alter 操作: %r' % op)
    rc, out, err = psql(sql)
    if rc != 0:
        raise RuntimeError('修改失败: ' + (err or out).strip()[-500:])
    try:
        if op == 'dropcol' and name:
            # 删列后同步清理该列的外键登记（残留会导致下次生成函数引用已删列）
            psql("DELETE FROM public.fk_meta WHERE fk_schema='%s' AND fk_table='%s' AND fk_column='%s';"
                 % (schema, table, name))
        elif op == 'renamecol':
            psql("UPDATE public.fk_meta SET fk_column='%s' WHERE fk_schema='%s' AND fk_table='%s' AND fk_column='%s';"
                 % (new, schema, table, old))
    except Exception:
        pass
    return {'ok': True, 'sql': sql}


def _gen_action_ids(table):
    info = LAST_GEN.get(table) or {}
    d = info.get('def') or {}
    acts = d.get('actions') or {}
    ids = [d.get('menu_action')] + [acts[k] for k in ('add', 'edit', 'merge', 'del', 'undel', 'query') if k in acts]
    return [i for i in ids if isinstance(i, int)]


def drop_table(body):
    schema, table = ident(body.get('schema') or 'sysinfo'), ident(body.get('table'))
    steps = []
    if body.get('with_funcs'):
        fns = qjson("select p.proname as name, pg_get_function_identity_arguments(p.oid) as args "
                    "from pg_proc p join pg_namespace n on n.oid=p.pronamespace "
                    "where n.nspname='%s' and p.proname in ('%s_query','%s_add','%s_edit','%s_merge','%s_del','%s_undel')"
                    % (schema, table, table, table, table, table, table))
        for f in fns:
            steps.append('DROP FUNCTION IF EXISTS %s.%s(%s);' % (schema, f['name'], f['args']))
        ids = _gen_action_ids(table)
        if ids:
            steps.append('DELETE FROM sysinfo.sysaction WHERE sysactionid IN (%s);' % ','.join(str(i) for i in ids))
        # 同步清理 actions 具体权限表（发布时按 actionurl 判重写入，删表时按 6 个操作后缀精确移除）
        ops = ('query', 'add', 'edit', 'merge', 'del', 'undel')
        urls = ','.join("'%s/%s_%s'" % (schema, table, op) for op in ops)
        steps.append('DELETE FROM sysinfo.actions WHERE actionurl IN (%s);' % urls)
    steps.append('DROP TABLE IF EXISTS %s.%s;' % (schema, table))
    if body.get('child_table'):
        try:
            steps.append('DROP TABLE IF EXISTS %s.%s;' % (schema, ident(body.get('child_table'))))
        except ValueError:
            pass
    rc, out, err = psql('\n'.join(steps))
    if rc != 0:
        raise RuntimeError('删除失败: ' + (err or out).strip()[-500:])
    # 清理外键元数据（该表作为引用方或被引用主表的配置均已无意义）；失败不阻断删表
    try:
        psql("DELETE FROM public.fk_meta WHERE (fk_schema='%s' AND fk_table='%s') "
             "OR (master_schema='%s' AND master_table='%s');" % (schema, table, schema, table))
    except Exception:
        pass
    # 同步移除已发布到前端的模块配置（modules.js），否则菜单/模块页面残留
    try:
        unpublish_module(table)
    except Exception:
        pass
    LAST_GEN.pop(table, None)
    _save_lastgen()
    return {'ok': True, 'sql': '\n'.join(steps)}



# ----------------------------- 模板生成与发布 -----------------------------

def build_def(body):
    kind = body.get('kind') or 'single'
    if kind not in ('single', 'parent_child', 'tree'):
        raise ValueError('kind 必须是 single/parent_child/tree')
    schema = ident(body.get('schema') or 'sysinfo')
    table = ident(body.get('table'))
    if not table_exists(schema, table):
        raise ValueError('表 %s.%s 不存在（请先建表）' % (schema, table))
    pk = ident(body.get('pk') or table_pk(schema, table) or (table + 'id'))
    cols = table_columns(schema, table)
    names = {c['name'] for c in cols}
    if pk not in names:
        raise ValueError('主键列 %s 不存在' % pk)
    if kind == 'tree':
        tmiss = [c for c in ('upid', 'idpath', 'idlevel', 'idcount') if c not in names]
        if tmiss:
            raise ValueError('树表缺少结构列: %s（请先用"修改表"加列，或新建表时选择树表类型）' % ','.join(tmiss))
    struct = {pk, 'isused', 'systemid'}
    if kind == 'tree':
        struct |= {'upid', 'idpath', 'idlevel', 'idcount'}
    fields = body.get('fields') or []
    if not fields:
        fields = [{'name': c['name'],
                   'type': c['ptype'],
                   'cn': c.get('comment') or c['name'],
                   'flag': c['name'] == 'isused'}
                  for c in cols if c['name'] not in struct]
    colmap = {c['name']: c for c in cols}
    for f in fields:
        name = ident(f.get('name'))
        if not f.get('type'):
            c = colmap.get(name)
            f['type'] = c['ptype'] if c else 'text'
        typ(f['type'])
    group = int(body.get('group') or 9001)
    prow = qjson("select idpath from sysinfo.sysaction where sysactionid=%d and isused=1" % group)
    if not prow:
        raise ValueError('权限节点 %s 不存在或已停用，请在权限树中选择挂载位置' % group)
    sug = suggest_ids()
    menu_action = int(body.get('menu_action') or sug['menu_action'])
    # 载入该表已发布的权限节点（跨分组）：按任一操作的 actionurl 反查其操作节点，
    # 操作节点的 upid 即本表的菜单节点——重新生成时菜单 id 保持稳定，权限节点
    # 不再每次新建（发布时经 gen_sysaction 的 ON CONFLICT DO UPDATE 迁移到新挂载点）
    try:
        urls = ','.join("'%s/%s_%s'" % (schema, table, op) for op in ('add', 'edit', 'merge', 'del', 'undel', 'query'))
        ex = qval("select upid from sysinfo.sysaction where actionid in "
                  "(select actionid from sysinfo.actions where actionurl in (%s) and isused=1) "
                  "and actionid is not null and sysactionid<>%d limit 1" % (urls, menu_action))
        if ex:
            menu_action = int(ex)
    except Exception:
        pass
    # 次选：复用同分组下同名的已有功能节点（无 actionid 的菜单节点，如仅手动建过菜单未发函数）
    try:
        ex = qval("select sysactionid from sysinfo.sysaction where upid=%d and sysactionname='%s' "
                  "and actionid is null and sysactionid<>%d and isused=1 limit 1"
                  % (group, str(body.get('chinese') or table).replace("'", "''"), menu_action))
        if ex:
            menu_action = int(ex)
    except Exception:
        pass
    actions = body.get('actions')
    if not actions:
        # 优先复用该表已发布的操作权限点（按 actionurl 匹配），避免重复发布时权限点漂移
        # （此前每次发布都取 max+1，导致 modules.js 的 ops 与函数 check_login 的权限点不一致）
        reused = {}
        for op in ('add', 'edit', 'merge', 'del', 'undel', 'query'):
            r = qval("select actionid from sysinfo.actions where actionurl='%s/%s_%s' order by actionid limit 1"
                     % (schema, table, op))
            if r:
                reused[op] = int(r)
        if len(reused) >= 4:
            actions = {op: reused.get(op) or sug['actions'][op]
                       for op in ('add', 'edit', 'merge', 'del', 'undel', 'query')}
        else:
            actions = sug['actions']
    if kind == 'parent_child' and body.get('existing'):
        actions.pop('undel', None)   # 父子表联动模块固定 5 操作（query/add/edit/merge/del），无 undel
    need_ops = (('add', 'edit', 'merge', 'del', 'query') if (kind == 'parent_child' and body.get('existing'))
                else ('add', 'edit', 'merge', 'del', 'undel', 'query'))
    for op in need_ops:
        if op not in actions:
            raise ValueError('actions 缺少 ' + op)
    d = {'kind': kind, 'schema': schema, 'table': table,
         'chinese': str(body.get('chinese') or table).strip()[:60],
         'pk': pk, 'group': str(group),
         'parent_idpath': prow[0]['idpath'],
         'icon': str(body.get('icon') or 'list'), 'systemid': int(body.get('systemid') or 100),
         'menu_action': menu_action, 'actions': actions, 'fields': fields,
         'has_isused': 'isused' in names, 'has_sysid': 'systemid' in names}
    if kind == 'parent_child':
        ct = ident(body.get('child_table'))
        if ct == table:
            raise ValueError('子表不能与父表相同')
        if not table_exists(schema, ct):
            raise ValueError('子表 %s.%s 不存在（请先建表）' % (schema, ct))
        cpk = ident(body.get('child_pk') or table_pk(schema, ct) or (ct + 'id'))
        ccols = table_columns(schema, ct)
        cnames = {c['name'] for c in ccols}
        # 子表结构列按实际结构条件化（已有表可能没有 isused/systemid）
        d['child_hi'] = 'isused' in cnames
        d['child_hs'] = 'systemid' in cnames
        if not body.get('existing'):
            cmiss = [c for c in ('isused', 'systemid') if c not in cnames]
            if cmiss:
                raise ValueError('子表缺少结构列: %s' % ','.join(cmiss))
        if cpk not in cnames:
            raise ValueError('子表主键列 %s 不存在' % cpk)
        # 关联字段：默认与父表主键同名的子表字段；子表无该列时须显式指定
        cfk = ident(body.get('child_fk') or (pk if pk in cnames else 'upid'))
        if cfk not in cnames:
            raise ValueError('子表缺少与父表主键对应的字段 %s（请在子表列中选择关联字段）' % cfk)
        cfields = body.get('child_fields') or []
        if not cfields:
            cstruct = {cpk, cfk, 'isused', 'systemid'}
            cfields = [{'name': c['name'],
                        'type': (('varchar(%d)' % c['len']) if c['data_type'] == 'character varying' and c.get('len') else
                                 ('numeric(%s,%s)' % (c['prec'], c['scale'])) if c['data_type'] == 'numeric' and c.get('prec') else
                                 c['data_type']),
                        'cn': c.get('comment') or c['name'],
                        'flag': c['name'] == 'isused'}
                       for c in ccols if c['name'] not in cstruct]
        for f in cfields:
            ident(f.get('name'))
            typ(f.get('type') or 'text')
        d.update(child_table=ct, child_pk=cpk, child_fk=cfk,
                 child_chinese=str(body.get('child_chinese') or ct).strip()[:60],
                 child_fields=cfields)
    # 函数/接口前缀：父子表联动模块（基于已有表）= 父表_子表，避免与父/子表各自的单表 CRUD 重名
    if kind == 'parent_child' and body.get('existing'):
        d['existing'] = 1
        d['fp'] = table + '_' + d.get('child_table', '')
    else:
        d['fp'] = table
    os.makedirs(DATA, exist_ok=True)
    with open(os.path.join(DATA, '_def.json'), 'w', encoding='utf-8') as f:
        json.dump(d, f, ensure_ascii=False)
    return gen_module.load_def(os.path.join(DATA, '_def.json'))


def _get_fks(d):
    """读取外键引用配置（public.fk_meta）；失败返回空列表"""
    try:
        return gen_module.get_fks(d, qval)
    except Exception as e:
        print('外键配置读取失败:', e)
        return []


def _gen_sql_full(d, lang, fks=None):
    """生成 SQL 并处理出错代码：为表分配（去重）专属 errorcode，把函数体中的
       通用码 100018（记录不存在/主键已存在）替换为该表专属码，并附加去重 INSERT——
       避免新表报错时显示别的表（如“部门信息出错”）的错误消息。"""
    try:
        code, ins = gen_module.alloc_error_code(d, qval)
    except Exception as e:
        print('errorcode 取号失败，回退 100018:', e)
        code, ins = 100018, None
    sql = gen_pyfuncs.gen_all(d) if lang == 'plpython3u' else gen_module.gen_sql(d, fks)
    sql = gen_module.apply_error_code(sql, code)
    if ins:
        sql += '\n-- 出错代码（建模生成，已去重）\n' + ins
    return sql


def gen_module_code(body):
    d = build_def(body)
    # 父子表联动模块（基于已有表）暂以 plpgsql 生成（plpython3u 模板尚未适配联动函数前缀）
    lang = 'plpgsql' if d.get('existing') else ('plpython3u' if body.get('lang') == 'plpython3u' else 'plpgsql')
    fks = _get_fks(d)
    sql = _gen_sql_full(d, lang, fks)
    mjs = gen_module.gen_modules_js(d, fks)
    import time
    LAST_GEN[d['table']] = {'def': d, 'lang': lang, 'sql': sql, 'modules_js': mjs,
                            'ts': time.strftime('%F %T')}
    _save_lastgen()
    return {'ok': True, 'lang': lang, 'table': d['table'], 'chinese': d['chinese'],
            'sql': sql, 'modules_js': mjs, 'def': d,
            'preview_url': '/static/preview.html?table=' + d['table']}


def perm_node(sid):
    """权限节点详情：sysaction 树节点 + actions 具体权限（若有）"""
    sid = int(sid)
    node = qjson("select sysactionid, sysactionname, idpath, idlevel, upid, actionid, "
                 "params, systemid, isused from sysinfo.sysaction where sysactionid=%d" % sid)
    if not node:
        raise ValueError('权限节点 %s 不存在' % sid)
    act = qjson("select actionid, actionname, description, params, actionurl, code, isused "
                "from sysinfo.actions where actionid=%d" % int(node[0]['actionid'] or 0)) \
        if node[0]['actionid'] else []
    return {'ok': True, 'node': node[0], 'action': act[0] if act else None}


def perm_update(body):
    """修改权限：同一事务内更新 sysaction（树）与 actions（具体权限）两表"""
    sid = int(body['sysactionid'])
    node = qjson("select sysactionid, actionid from sysinfo.sysaction where sysactionid=%d" % sid)
    if not node:
        raise ValueError('权限节点 %s 不存在' % sid)
    aid = node[0]['actionid']
    steps = []
    ssets = []
    if body.get('sysactionname') is not None:
        ssets.append('sysactionname=%s' % lit(str(body['sysactionname']).strip()[:60]))
    if body.get('isused') is not None:
        ssets.append('isused=%d' % int(body['isused']))
    if ssets:
        steps.append('UPDATE sysinfo.sysaction SET %s WHERE sysactionid=%d;' % (','.join(ssets), sid))
    asets = []
    for k, col in (('actionname', 'actionname'), ('description', 'description'),
                   ('actionurl', 'actionurl'), ('params', 'params'), ('code', 'code')):
        if body.get(k) is not None:
            asets.append('%s=%s' % (col, lit(str(body[k]).strip()[:200])))
    if body.get('action_isused') is not None:
        asets.append('isused=%d' % int(body['action_isused']))
    if asets:
        if not aid:
            raise ValueError('该节点无具体权限（actionid 为空），仅可修改树节点信息')
        has = qval("select count(*) from sysinfo.actions where actionid=%d" % int(aid))
        if has != '1':
            raise ValueError('actions 表中不存在 actionid=%s 的具体权限记录' % aid)
        steps.append('UPDATE sysinfo.actions SET %s WHERE actionid=%d;' % (','.join(asets), int(aid)))
    if not steps:
        return {'ok': True, 'changed': 0}
    rc, out, err = psql('BEGIN;\n' + '\n'.join(steps) + '\nCOMMIT;')
    if rc != 0:
        raise RuntimeError('更新失败（事务已回滚）: ' + (err or out).strip()[-500:])
    return {'ok': True, 'changed': len(steps), 'sysaction_updated': bool(ssets),
            'actions_updated': bool(asets)}


def perm_add(body):
    """在选中权限节点下新增子节点（仅写 sysaction 树；不创建 actions 具体权限）"""
    parent = int(body['parent'])
    name = str(body.get('name') or '').strip()
    if not name:
        raise ValueError('请填写节点名称')
    p = qjson("select sysactionid, idpath, idlevel, systemid from sysinfo.sysaction "
              "where sysactionid=%d and isused=1" % parent)
    if not p:
        raise ValueError('父权限节点不存在或已停用')
    p = p[0]
    newid = int(qval("select coalesce(max(sysactionid),100)+1 from sysinfo.sysaction"))
    # 避免复用历史 idpath 中引用过的 ID（如已删功能的孤儿子节点残留），
    # 否则新节点会误挂上历史子节点（例：复用 9113 时，残留的 224 会挂到新节点下）
    used_paths = qval("select coalesce(string_agg(idpath, ' '), '') from sysinfo.sysaction")
    while ('.%d.' % newid) in ('.%s.' % used_paths.replace(' ', '.')):
        newid += 1
    steps = ["INSERT INTO sysinfo.sysaction (isused,idpath,idlevel,idcount,upid,params,systemid,"
             "sysactionid,sysactionname,actionid,isdefault) VALUES (1,'%s.%d',%d,0,%d,NULL,%d,%d,%s,NULL,0);"
             % (p['idpath'], newid, p['idlevel'] + 1, p['sysactionid'],
                p['systemid'] or 100, newid, lit(name))]
    steps.append("UPDATE sysinfo.sysaction SET idcount=(select count(*) from sysinfo.sysaction c "
                 "where c.upid=%d and c.isused=1) WHERE sysactionid=%d;" % (parent, parent))
    rc, out, err = psql('BEGIN;\n' + '\n'.join(steps) + '\nCOMMIT;')
    if rc != 0:
        raise RuntimeError('新增失败（事务已回滚）: ' + (err or out).strip()[-500:])
    return {'ok': True, 'sysactionid': newid}


def perm_delete(body):
    """物理删除选中节点及其整个子树（sysaction 行直接删除，actions 具体权限同步删除）"""
    sid = int(body['sysactionid'])
    node = qjson("select idpath from sysinfo.sysaction where sysactionid=%d" % sid)
    if not node:
        raise ValueError('权限节点不存在')
    if sid in (100, 9001, 9002, 9003):
        raise ValueError('系统核心节点禁止删除')
    p = node[0]['idpath']
    cnt = int(qval("select count(*) from sysinfo.sysaction where idpath=%s OR idpath LIKE %s"
                   % (lit(p), lit(p + '.%'))))
    steps = ["DELETE FROM sysinfo.actions WHERE actionid IN "
             "(select actionid from sysinfo.sysaction where (idpath=%s OR idpath LIKE %s) AND actionid IS NOT NULL);"
             % (lit(p), lit(p + '.%')),
             "DELETE FROM sysinfo.sysaction WHERE idpath=%s OR idpath LIKE %s;"
             % (lit(p), lit(p + '.%'))]
    rc, out, err = psql('BEGIN;\n' + '\n'.join(steps) + '\nCOMMIT;')
    if rc != 0:
        raise RuntimeError('删除失败（事务已回滚）: ' + (err or out).strip()[-500:])
    return {'ok': True, 'affected': cnt}


def perm_move(body):
    """交换两个同级权限节点的 idpath 前缀（树上顺序由 idpath 字典序决定，
    交换前缀即交换显示顺序；id/upid/actionid 均不变）"""
    sid = int(body['sysactionid'])
    tid = int(body['target'])
    if sid == tid:
        return {'ok': False, 'detail': '不能与自身交换'}
    rows = qjson("select sysactionid, upid, idpath, idlevel from sysinfo.sysaction "
                 "where sysactionid IN (%d,%d) and isused=1" % (sid, tid))
    if len(rows) < 2:
        raise ValueError('权限节点不存在或已停用')
    a = [r for r in rows if r['sysactionid'] == sid][0]
    b = [r for r in rows if r['sysactionid'] == tid][0]
    if a['upid'] != b['upid'] or a['idlevel'] != b['idlevel']:
        return {'ok': False, 'detail': '仅支持同级（同一父节点）的权限节点间移动'}
    pA, pB = a['idpath'], b['idpath']
    tmp = '@tmp1@'
    steps = [
        # node 子树：pA → 临时段
        "UPDATE sysinfo.sysaction SET idpath='%s' || substr(idpath, %d) "
        "WHERE idpath=%s OR idpath LIKE %s;" % (tmp, len(pA) + 1, lit(pA), lit(pA + '.%')),
        # other 子树：pB → pA
        "UPDATE sysinfo.sysaction SET idpath='%s' || substr(idpath, %d) "
        "WHERE idpath=%s OR idpath LIKE %s;" % (pA, len(pB) + 1, lit(pB), lit(pB + '.%')),
        # node 子树：临时段 → pB
        "UPDATE sysinfo.sysaction SET idpath='%s' || substr(idpath, %d) "
        "WHERE idpath=%s OR idpath LIKE %s;" % (pB, len(tmp) + 1, lit(tmp), lit(tmp + '.%')),
    ]
    rc, out, err = psql('BEGIN;\n' + '\n'.join(steps) + '\nCOMMIT;')
    if rc != 0:
        raise RuntimeError('移动失败（事务已回滚）: ' + (err or out).strip()[-500:])
    return {'ok': True, 'moved': [sid, tid]}


def systems_list():
    """系统列表（不含 prikey 明文）"""
    return {'ok': True, 'systems': qjson(
        "select systemid, systemname, isused, algorithm, loginname from sysinfo.systeminfo order by systemid")}


def system_create(body):
    sid = int(body.get('systemid') or qval("select coalesce(max(systemid),100)+1 from sysinfo.systeminfo"))
    name = str(body.get('systemname') or '').strip()
    if not name:
        raise ValueError('请填写系统名称')
    if qval("select count(*) from sysinfo.systeminfo where systemid=%d" % sid) == '1':
        raise ValueError('系统 %s 已存在' % sid)
    algo = int(body.get('algorithm') or 1)
    prikey = str(body.get('prikey') or '').strip()
    if not prikey:
        raise ValueError('请填写 prikey（该系统数据加密密钥）')
    steps = ["INSERT INTO sysinfo.systeminfo (systemid, systemname, isused, algorithm, prikey, "
             "createoperator, createtime) VALUES (%d, %s, 1, %d, %s, 0, now());"
             % (sid, lit(name), algo, lit(prikey))]
    rc, out, err = psql('BEGIN;\n' + '\n'.join(steps) + '\nCOMMIT;')
    if rc != 0:
        raise RuntimeError('新增失败（事务已回滚）: ' + (err or out).strip()[-500:])
    return {'ok': True, 'systemid': sid}


def system_update(body):
    sid = int(body['systemid'])
    sets = []
    if body.get('systemname') is not None:
        sets.append('systemname=%s' % lit(str(body['systemname']).strip()[:60]))
    if body.get('isused') is not None:
        sets.append('isused=%d' % int(body['isused']))
    if not sets:
        return {'ok': True, 'changed': 0}
    rc, out, err = psql('BEGIN;\nUPDATE sysinfo.systeminfo SET %s WHERE systemid=%d;\nCOMMIT;'
                        % (','.join(sets), sid))
    if rc != 0:
        raise RuntimeError('修改失败（事务已回滚）: ' + (err or out).strip()[-500:])
    return {'ok': True, 'changed': len(sets)}


def system_delete(body):
    sid = int(body['systemid'])
    if sid == 100:
        raise ValueError('默认系统（100 维护系统）禁止删除')
    ref = int(qval("select count(*) from sysinfo.sysaction where systemid=%d" % sid))
    if ref:
        raise ValueError('该系统下仍有 %d 个权限节点，禁止删除' % ref)
    rc, out, err = psql('BEGIN;\nDELETE FROM sysinfo.systeminfo WHERE systemid=%d;\nCOMMIT;' % sid)
    if rc != 0:
        raise RuntimeError('删除失败（事务已回滚）: ' + (err or out).strip()[-500:])
    return {'ok': True}


def perm_group(sid):
    """一个功能的整组权限：菜单节点 + 全部操作节点（含各自 actions 明细）"""
    sid = int(sid)
    node = qjson("select sysactionid, sysactionname, idpath, idlevel, upid, actionid, isused "
                 "from sysinfo.sysaction where sysactionid=%d and isused=1" % sid)
    if not node:
        raise ValueError('权限节点不存在或已停用')
    node = node[0]
    menu = node
    if node['idlevel'] >= 4:                      # 操作节点 → 上溯到菜单节点
        menu = qjson("select sysactionid, sysactionname, idpath, idlevel, upid, actionid, isused "
                     "from sysinfo.sysaction where sysactionid=%d" % node['upid'])[0]
    ops = qjson("select sysactionid, sysactionname, idpath, idlevel, upid, actionid, isused "
                "from sysinfo.sysaction where upid=%d and isused=1 order by idpath" % menu['sysactionid'])
    aids = ','.join(str(o['actionid']) for o in ops if o['actionid']) or '0'
    acts = qjson("select actionid, actionname, description, actionurl, params, code, isused "
                 "from sysinfo.actions where actionid IN (%s)" % aids)
    amap = {a['actionid']: a for a in acts}
    out_ops = [{'sysactionid': o['sysactionid'], 'sysactionname': o['sysactionname'],
                'idpath': o['idpath'], 'actionid': o['actionid'], 'isused': o['isused'],
                'action': amap.get(o['actionid'])} for o in ops]
    return {'ok': True, 'menu': menu, 'ops': out_ops}


def perm_update_group(body):
    """批量保存一个功能的整组权限：菜单节点 + 操作节点表格（sysaction 与 actions 两表同事务）"""
    menu = body.get('menu') or {}
    ops = body.get('ops') or []
    if not menu.get('sysactionid'):
        raise ValueError('缺少菜单节点')
    mid = int(menu['sysactionid'])
    mrow = qjson("select idpath from sysinfo.sysaction where sysactionid=%d" % mid)
    if not mrow:
        raise ValueError('菜单节点不存在')
    mpath = mrow[0]['idpath']
    steps = []
    msets = []
    if menu.get('sysactionname') is not None:
        msets.append('sysactionname=%s' % lit(str(menu['sysactionname']).strip()[:60]))
    if menu.get('isused') is not None:
        msets.append('isused=%d' % int(menu['isused']))
    if msets:
        steps.append('UPDATE sysinfo.sysaction SET %s WHERE sysactionid=%d;' % (','.join(msets), mid))
    for op in ops:
        osid = int(op['sysactionid'])
        orow = qjson("select idpath from sysinfo.sysaction where sysactionid=%d" % osid)
        if not orow or not (orow[0]['idpath'] == mpath or orow[0]['idpath'].startswith(mpath + '.')):
            raise ValueError('操作节点 %s 不属于菜单 %s' % (osid, mid))
        osets = []
        if op.get('sysactionname') is not None:
            osets.append('sysactionname=%s' % lit(str(op['sysactionname']).strip()[:60]))
        if op.get('isused') is not None:
            osets.append('isused=%d' % int(op['isused']))
        if osets:
            steps.append('UPDATE sysinfo.sysaction SET %s WHERE sysactionid=%d;' % (','.join(osets), osid))
        act = op.get('action') or {}
        if op.get('actionid') and act:
            aid = int(op['actionid'])
            asets = []
            for k in ('actionname', 'description', 'actionurl'):
                if act.get(k) is not None:
                    asets.append('%s=%s' % (k, lit(str(act[k]).strip()[:200])))
            if asets:
                aex = qval("select count(*) from sysinfo.actions where actionid=%d" % aid)
                if aex == '1':
                    steps.append('UPDATE sysinfo.actions SET %s WHERE actionid=%d;'
                                 % (','.join(asets), aid))
    if not steps:
        return {'ok': True, 'changed': 0}
    rc, out, err = psql('BEGIN;\n' + '\n'.join(steps) + '\nCOMMIT;')
    if rc != 0:
        raise RuntimeError('更新失败（事务已回滚）: ' + (err or out).strip()[-500:])
    return {'ok': True, 'changed': len(steps)}


def perm_tree():
    """sysinfo.sysaction 权限树（扁平按 idpath 排序，前端按层级缩进渲染）"""
    rows = qjson("select sysactionid, sysactionname, idpath, idlevel, upid, isused "
                 "from sysinfo.sysaction order by idpath")
    return {'ok': True, 'nodes': rows}


def suggest_ids():
    menu = qval("select coalesce(max(sysactionid),9200) from sysinfo.sysaction where sysactionid between 9000 and 9899")
    act = qval("select coalesce(max(sysactionid),205) from sysinfo.sysaction where sysactionid < 9000")
    m, a = int(menu) + 1, int(act) + 1
    return {'menu_action': m, 'actions': {'add': a, 'edit': a + 1, 'merge': a + 2, 'del': a + 3, 'undel': a + 4, 'query': a + 5}}




def _save_lastgen():
    try:
        os.makedirs(DATA, exist_ok=True)
        slim = {k: {kk: vv for kk, vv in v.items() if kk != 'sql'}
                for k, v in LAST_GEN.items()}
        with open(os.path.join(DATA, 'lastgen.json'), 'w', encoding='utf-8') as f:
            json.dump(slim, f, ensure_ascii=False, indent=1)
    except Exception as e:  # noqa: BLE001
        print('lastgen 保存失败:', e)


def _load_lastgen():
    p = os.path.join(DATA, 'lastgen.json')
    if os.path.exists(p):
        try:
            for k, v in json.load(open(p, encoding='utf-8')).items():
                LAST_GEN.setdefault(k, v)
        except Exception as e:  # noqa: BLE001
            print('lastgen 读取失败:', e)


def patch_modules_js(table, block):
    """在项目与部署目录的 modules.js 中替换/追加该表生成块（幂等）"""
    pat = re.compile(r'/\* %s（.*?\n(?:.*\n)*?  \{add:.*?\);\n?'
                     r'(?:/\* 父子表：.*?\n(?:.*\n)*?SUBTABLECFG\.push\([^\n]*\);\n?)?' % re.escape(table))
    targets = [os.path.join(PROJ, 'html', 'assets', 'modules.js'),
               os.path.join(CFG['deploy_dir'], 'assets', 'modules.js')]
    results = []
    for mp in targets:
        if not os.path.exists(mp):
            src = os.path.join(PROJ, 'html', 'assets', 'modules.js')
            if os.path.exists(src):
                os.makedirs(os.path.dirname(mp), exist_ok=True)
                shutil.copy(src, mp)
            else:
                continue
        text = open(mp, encoding='utf-8').read()
        text = pat.sub('', text)
        if not text.endswith('\n'):
            text += '\n'
        text += '\n' + block
        write_file(mp, text)
        results.append(mp)
    return results


def unpublish_module(table):
    """从项目与部署目录的 modules.js 中移除该表的生成块（前端菜单/页面随之消失），并升级资源版本号"""
    pat = re.compile(r'/\* %s（.*?\n(?:.*\n)*?  \{add:.*?\);\n?'
                     r'(?:/\* 父子表：.*?\n(?:.*\n)*?SUBTABLECFG\.push\([^\n]*\);\n?)?' % re.escape(table))
    for mp in (os.path.join(PROJ, 'html', 'assets', 'modules.js'),
               os.path.join(CFG['deploy_dir'], 'assets', 'modules.js')):
        if not os.path.exists(mp):
            continue
        text = open(mp, encoding='utf-8').read()
        new = pat.sub('', text)
        if new != text:
            write_file(mp, new)
    bump_index()


def bump_index():
    pat = re.compile(r'v=(\d+)')
    for p in (os.path.join(PROJ, 'html', 'index.html'),
              os.path.join(CFG['deploy_dir'], 'index.html')):
        if os.path.exists(p):
            s = open(p, encoding='utf-8').read()
            write_file(p, pat.sub(lambda m: 'v=%d' % (int(m.group(1)) + 1), s))


def hot_reload():
    """刷新 nginx postgres 模块函数缓存：HTTP 热重载 → sudo nginx -s reload"""
    urls = [CFG['reload_url'], CFG['reload_url'].replace('https://', 'http://')]
    ctx = ssl._create_unverified_context()
    last = ''
    for u in urls:
        try:
            tok = ('?token=' + CFG['reload_token']) if CFG['reload_token'] else ''
            req = urllib.request.Request(u + tok, data=b'', method='POST')
            body = urllib.request.urlopen(req, timeout=6, context=ctx).read().decode('utf-8', 'replace')
            if '"errorcode":0' in body or 'reloaded' in body:
                return {'ok': True, 'method': 'http', 'detail': body.strip()[:200], 'url': u}
            last = '%s => %s' % (u, body.strip()[:120])
        except Exception as e:  # noqa: BLE001
            last = '%s: %s' % (u, e)
    try:
        r = subprocess.run(['sudo', '-n', '/opt/nginx/sbin/nginx', '-s', 'reload'],
                           capture_output=True, text=True, timeout=15)
        if r.returncode == 0:
            return {'ok': True, 'method': 'sudo', 'detail': 'nginx -s reload 完成'}
        last = 'sudo: ' + (r.stderr or '').strip()[:200]
    except Exception as e:  # noqa: BLE001
        last = 'sudo: %s' % e
    return {'ok': False, 'method': 'manual', 'detail': last or '未知错误'}


def publish_module(body):
    d = build_def(body)
    # 父子表联动模块（基于已有表）暂以 plpgsql 生成（plpython3u 模板尚未适配联动函数前缀）
    lang = 'plpgsql' if d.get('existing') else ('plpython3u' if body.get('lang') == 'plpython3u' else 'plpgsql')
    fks = _get_fks(d)
    sql = _gen_sql_full(d, lang, fks)
    mjs = gen_module.gen_modules_js(d, fks)
    import time
    ts = time.strftime('%Y%m%d%H%M%S')
    bdir = os.path.join(DATA, 'backup', ts)
    os.makedirs(bdir, exist_ok=True)
    for p in (os.path.join(PROJ, 'html', 'assets', 'modules.js'),
              os.path.join(PROJ, 'html', 'index.html'),
              os.path.join(CFG['deploy_dir'], 'assets', 'modules.js'),
              os.path.join(CFG['deploy_dir'], 'index.html')):
        if os.path.exists(p):
            shutil.copy(p, os.path.join(bdir, os.path.basename(p) + '.bak'))
    pre = ("DO $$ DECLARE r record; BEGIN\n"
           "  FOR r IN SELECT p.oid::regprocedure::text AS sig FROM pg_proc p "
           "JOIN pg_namespace n ON n.oid=p.pronamespace "
           "WHERE n.nspname='%s' AND p.proname IN (%s) LOOP\n"
           "    EXECUTE 'DROP FUNCTION ' || r.sig;\n"
           "  END LOOP;\nEND $$;") % (d['schema'],
           ','.join("'%s_%s'" % (d['table'], op) for op in ('query', 'add', 'edit', 'merge', 'del', 'undel')))
    rc, out, err = psql('BEGIN;\n' + pre + '\n' + sql + '\nCOMMIT;')
    if rc != 0:
        raise RuntimeError('SQL 应用失败（事务已回滚）: ' + (err or out).strip()[-800:])
    files = patch_modules_js(d['table'], mjs)
    bump_index()
    LAST_GEN[d['table']] = {'def': d, 'lang': lang, 'ts': time.strftime('%F %T')}
    _save_lastgen()
    rel = hot_reload()
    sub = 'ngxdb/' if os.path.realpath(CFG['deploy_dir']).endswith('/ngxdb') else ''
    return {'ok': True, 'table': d['table'], 'chinese': d['chinese'], 'lang': lang,
            'sql_applied': True, 'files': files, 'reload': rel, 'backup': bdir,
            'ngxdb_url': '/' + sub}


def list_published():
    mp = os.path.join(CFG['deploy_dir'], 'assets', 'modules.js')
    if not os.path.exists(mp):
        mp = os.path.join(PROJ, 'html', 'assets', 'modules.js')
    text = open(mp, encoding='utf-8').read()
    out = []
    pat = re.compile(r"/\* (\w+)（([^）]*)）：由 tools/gen_module\.py 生成 \*/\s*\n"
                     r"P\('([^']+)','([^']*)','([^']*)',(\d+),'([^']*)','([^']*)'")
    for m in pat.finditer(text):
        t = m.group(1)
        info = LAST_GEN.get(t) or {}
        out.append({'table': t, 'chinese': m.group(2), 'id': m.group(3),
                    'group': m.group(4), 'menu': int(m.group(5)), 'icon': m.group(6),
                    'base': m.group(7), 'lang': info.get('lang', 'plpgsql'),
                    'ts': info.get('ts', ''), 'in_db': bool(info)})
    return out


def preview_config(table):
    if table in LAST_GEN and 'def' in LAST_GEN[table]:
        d = LAST_GEN[table]['def']
        return {'table': d['table'], 'schema': d['schema'], 'chinese': d['chinese'],
                'pk': d['pk'], 'fields': d['fields'], 'group': d['group'],
                'kind': d.get('kind', 'single'),
                'menu_action': d['menu_action'], 'actions': d['actions'], 'from': 'lastgen'}
    schema = 'sysinfo'
    if not table_exists(schema, table):
        for t in list_tables():
            if t['table'] == table:
                schema = t['schema']
                break
    if not table_exists(schema, table):
        raise ValueError('未知表: ' + table)
    pk = table_pk(schema, table) or 'id'
    struct = {pk, 'isused', 'systemid'}
    fields = [{'name': c['name'], 'type': c['data_type'], 'cn': c.get('comment') or c['name'],
               'flag': c['name'] == 'isused'}
              for c in table_columns(schema, table) if c['name'] not in struct]
    return {'table': table, 'schema': schema, 'chinese': table, 'pk': pk,
            'fields': fields, 'group': '9001', 'menu_action': 0, 'actions': {}, 'from': 'derived'}


def preview_data(schema, table, pk, limit):
    limit = max(1, min(int(limit or 20), 100))
    return qjson('select * from %s.%s order by %s desc limit %d' % (schema, table, pk, limit))


# ----------------------------- HTTP 服务 -----------------------------

MIME = {'.html': 'text/html; charset=utf-8', '.js': 'application/javascript; charset=utf-8',
        '.css': 'text/css; charset=utf-8', '.json': 'application/json; charset=utf-8',
        '.png': 'image/png', '.svg': 'image/svg+xml', '.ico': 'image/x-icon'}


class Handler(BaseHTTPRequestHandler):
    server_version = 'ngxdb-admin/1.0'

    def log_message(self, fmt, *args):  # noqa: A003
        sys.stderr.write('[admin] %s %s\n' % (self.address_string(), fmt % args))

    def _cors(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')

    def _json(self, obj, status=200):
        data = json.dumps(obj, ensure_ascii=False).encode('utf-8')
        self.send_response(status)
        self._cors()
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _static(self, rel):
        path = os.path.realpath(os.path.join(STATIC, rel))
        root = os.path.realpath(STATIC)
        if not path.startswith(root + os.sep) and path != root:
            return self._json({'error': '非法路径'}, 403)
        if not os.path.isfile(path):
            return self._json({'error': 'not found'}, 404)
        ext = os.path.splitext(path)[1]
        with open(path, 'rb') as f:
            data = f.read()
        self.send_response(200)
        self._cors()
        self.send_header('Content-Type', MIME.get(ext, 'application/octet-stream'))
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Cache-Control', 'no-cache')
        self.end_headers()
        self.wfile.write(data)

    def _body(self):
        n = int(self.headers.get('Content-Length') or 0)
        raw = self.rfile.read(n) if n else b'{}'
        try:
            return json.loads(raw.decode('utf-8') or '{}')
        except Exception:  # noqa: BLE001
            return dict(urllib.parse.parse_qsl(raw.decode('utf-8')))

    def do_OPTIONS(self):  # noqa: N802
        self.send_response(204)
        self._cors()
        self.send_header('Content-Length', '0')
        self.end_headers()

    # ---- 路由 ----
    def do_GET(self):  # noqa: N802
        u = urllib.parse.urlparse(self.path)
        q = dict(urllib.parse.parse_qsl(u.query))
        try:
            if u.path == '/' or u.path == '/index.html':
                return self._static('index.html')
            if u.path.startswith('/static/'):
                return self._static(u.path[len('/static/'):])
            if u.path == '/favicon.ico':
                return self._static('favicon.svg')
            if u.path == '/api/ping':
                return self._json({'ok': True, 'app': 'ngxdb-admin', 'version': '1.0'})
            if u.path == '/api/info':
                return self._info()
            if u.path == '/api/schemas':
                rows = qjson("select nspname from pg_namespace where nspname not like 'pg_%' "
                             "and nspname not in ('information_schema','cron','private','myself','crypto') order by 1")
                return self._json({'ok': True, 'schemas': [r['nspname'] for r in rows]})
            if u.path == '/api/tables':
                return self._json({'ok': True, 'tables': list_tables(q.get('schema') or None)})
            if u.path == '/api/table':
                s, t = ident(q.get('schema') or 'sysinfo'), ident(q.get('table'))
                if not table_exists(s, t):
                    return self._json({'error': '表不存在'}, 404)
                return self._json({'ok': True, 'schema': s, 'table': t,
                                   'comment': table_comment(s, t),
                                   'pk': table_pk(s, t), 'columns': table_columns(s, t),
                                   'stat': table_rows_size(s, t)})
            if u.path == '/api/ids':
                return self._json({'ok': True, **suggest_ids()})
            if u.path == '/api/perm/tree':
                return self._json(perm_tree())
            if u.path == '/api/perm/group':
                return self._json(perm_group(q.get('sysactionid')))
            if u.path == '/api/systems':
                return self._json(systems_list())
            if u.path == '/api/perm/node':
                return self._json(perm_node(q.get('sysactionid')))
            if u.path == '/api/published':
                return self._json({'ok': True, 'modules': list_published()})
            if u.path == '/api/preview/config':
                return self._json({'ok': True, **preview_config(ident(q.get('table')))})
            if u.path == '/api/preview/data':
                s, t, p = ident(q.get('schema')), ident(q.get('table')), ident(q.get('pk') or 'id')
                return self._json({'ok': True, 'rows': preview_data(s, t, p, q.get('limit'))})
            if u.path == '/api/fk':
                return self._json({'ok': True, 'fks': list_fks(ident(q.get('schema')), ident(q.get('table')))})
            return self._json({'error': 'not found'}, 404)
        except Exception as e:  # noqa: BLE001
            traceback.print_exc()
            return self._json({'error': str(e)}, 400 if isinstance(e, ValueError) else 500)

    def do_POST(self):  # noqa: N802
        u = urllib.parse.urlparse(self.path)
        try:
            body = self._body()
            if u.path == '/api/table/create':
                return self._json(create_table(body))
            if u.path == '/api/fk/save':
                return self._json(save_fks(body))
            if u.path == '/api/fkfdw/setup':
                return self._json(fkfdw_setup(body))
            if u.path == '/api/fkfdw/rewrite':
                return self._json(fkfdw_rewrite(body))
            if u.path == '/api/schema/create':
                return self._json(create_schema(body))
            if u.path == '/api/perm/update':
                return self._json(perm_update(body))
            if u.path == '/api/perm/update_group':
                return self._json(perm_update_group(body))
            if u.path == '/api/system/create':
                return self._json(system_create(body))
            if u.path == '/api/system/update':
                return self._json(system_update(body))
            if u.path == '/api/system/delete':
                return self._json(system_delete(body))
            if u.path == '/api/perm/add':
                return self._json(perm_add(body))
            if u.path == '/api/perm/delete':
                return self._json(perm_delete(body))
            if u.path == '/api/perm/move':
                return self._json(perm_move(body))
            if u.path == '/api/table/alter':
                return self._json(alter_table(body))
            if u.path == '/api/table/update_fields':
                return self._json(update_fields(body))
            if u.path == '/api/table/drop':
                return self._json(drop_table(body))
            if u.path == '/api/gen':
                return self._json(gen_module_code(body))
            if u.path == '/api/publish':
                return self._json(publish_module(body))
            if u.path == '/api/reload':
                return self._json(hot_reload())
            return self._json({'error': 'not found'}, 404)
        except Exception as e:  # noqa: BLE001
            traceback.print_exc()
            return self._json({'error': str(e)}, 400 if isinstance(e, ValueError) else 500)

    def _info(self):
        ver = qval('select version()').split(',')[0]
        try:
            if os.name == 'nt':   # Windows: pgrep 不存在，用 tasklist 统计
                nr = subprocess.run(['tasklist', '/FI', 'IMAGENAME eq nginx.exe', '/NH'],
                                    capture_output=True, text=True)
                workers = str(sum(1 for ln in nr.stdout.splitlines()
                                  if 'nginx' in ln.lower())) if nr.returncode == 0 else '0'
            else:
                nproc = subprocess.run(['pgrep', '-c', 'nginx'], capture_output=True, text=True)
                workers = nproc.stdout.strip() or '0'
        except Exception:
            workers = '0'
        det = detect_pg_db()
        return self._json({'ok': True,
                           'db': {'host': CFG['pg_host'], 'port': CFG['pg_port'],
                                  'name': CFG['pg_db'], 'user': CFG['pg_user'], 'version': ver,
                                  'detect': det},
                           'nginx': {'workers': workers,
                                     'deploy_dir': CFG['deploy_dir'],
                                     'deploy_exists': os.path.isdir(CFG['deploy_dir'])},
                           'admin': {'data_dir': DATA, 'lastgen': sorted(LAST_GEN.keys())}})


def main():
    _load_lastgen()
    det = detect_pg_db()
    print('业务库探测:', det)
    addr = (CFG['host'], CFG['port'])
    srv = ThreadingHTTPServer(addr, Handler)
    print('NGXDB 本地管理台已启动: http://%s:%d/  （仅限本机访问）' % addr)
    print('数据库: %s@%s:%s/%s   部署目录: %s' % (CFG['pg_user'], CFG['pg_host'],
                                                 CFG['pg_port'], CFG['pg_db'], CFG['deploy_dir']))
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()




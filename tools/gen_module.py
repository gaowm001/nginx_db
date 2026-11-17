#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""NGXDB 新业务模块一键生成器。

按一份 JSON 定义，生成/更新：
  1. extension/ngxdb_postgresql.sql —— 建表 + query/add/edit/merge/del/undel 六函数
     （含 COMMENT ON FUNCTION 注释）+ sysaction 权限节点（菜单+操作）INSERT；
  2. html/assets/modules.js —— P() 模块配置（菜单/列/表单/操作权限/树形标志）；
  3. html/index.html —— 静态资源版本号 +1；
  4. README.md —— 功能模块表、函数计数、新增模块记录；
  5. test/TEST_PLAN.md —— 函数计数、覆盖矩阵、测试场景。

用法：
  python3 tools/gen_module.py --def tools/example_notice.json            # 仅改文件
  python3 tools/gen_module.py --def tools/example_notice.json --apply    # 并执行 SQL 到在用库
  python3 tools/gen_module.py --def ... --apply-tx                       # 事务内试跑后回滚(仅验证)
  python3 tools/gen_module.py --def ... --deploy                         # 并部署 html 到 /opt/nginx/html/ngxdb

kind 取值：single(单表) / parent_child(父子表) / tree(树形表)
"""
import argparse, datetime, json, os, re, shutil, subprocess, sys, tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SQL_FILE = os.path.join(ROOT, 'extension', 'ngxdb_postgresql.sql')
HTML_DIR = os.path.join(ROOT, 'html')
# 部署目录：Linux 默认 /opt/nginx/html/ngxdb；Windows 默认 C:\nginx\html\ngxdb；可用环境变量覆盖
DEPLOY_DIR = os.environ.get('NGXDB_DEPLOY_DIR') or (
    '/opt/nginx/html/ngxdb' if os.name != 'nt' else r'C:\nginx\html\ngxdb')
PG = dict(host=os.environ.get('PGHOST', '127.0.0.1'),
          user=os.environ.get('PGUSER', 'postgres'),
          password=os.environ.get('PGPASSWORD', ''),
          db=os.environ.get('PGDATABASE', 'postgres'))
VERBS = {'add': '新增', 'edit': '修改', 'merge': '合并', 'del': '删除', 'undel': '恢复', 'query': '查询'}


def load_def(path):
    d = json.load(open(path, encoding='utf-8'))
    for k in ('kind', 'table', 'chinese', 'pk', 'menu_action', 'actions', 'fields'):
        if k not in d:
            raise ValueError('定义缺少必填项: ' + k)
    if d['kind'] not in ('single', 'parent_child', 'tree'):
        raise ValueError('kind 必须是 single/parent_child/tree')
    # 父子表联动模块（基于已有表，existing=1）固定 5 操作（query/add/edit/merge/del），无 undel；
    # 其余模块仍为 6 操作。校验失败抛 ValueError（勿用 sys.exit——SystemExit 不被
    # 管理台 do_POST 的 except Exception 捕获，会导致请求线程静默死亡、前端收到空响应）
    ops = (('add', 'edit', 'merge', 'del', 'query')
           if (d['kind'] == 'parent_child' and d.get('existing'))
           else ('add', 'edit', 'merge', 'del', 'undel', 'query'))
    for op in ops:
        if op not in d['actions']:
            raise ValueError('actions 缺少 ' + op)
    d.setdefault('schema', 'sysinfo')
    d.setdefault('group', '9001')
    d.setdefault('icon', 'list')
    d.setdefault('systemid', 100)
    d.setdefault('has_isused', True)   # 表是否含 isused 结构列（无则 del 为物理删除、无 undel）
    d.setdefault('has_sysid', True)    # 表是否含 systemid 结构列（无则不做系统归属）
    d.setdefault('fp', d['table'])     # 函数/接口前缀（父子表联动模块=父表_子表，避免与单表 CRUD 重名）
    if d['kind'] == 'parent_child':
        for k in ('child_table', 'child_pk', 'child_fields'):
            if not d.get(k):
                raise ValueError('parent_child 必须提供 ' + k)
        d.setdefault('child_fk', 'upid')
        d.setdefault('child_chinese', d['child_table'])
        # 父表“明细”挂载子表（app.js 的 SUBTABLE key = 子表表名）
        d['dt'] = d['child_table']
    return d


# 建表允许的列类型白名单（admin 建表/改列与 json_to_recordset 列定义共用）。
# 支持数组形式（如 bigint[]、text[]、varchar(32)[]）——用于外键多选列。
COL_TYPE_RE = re.compile(r'^(varchar\(\d+\)|char\(\d+\)|character\(\d+\)|text|integer|bigint|smallint|'
                         r'numeric\(\d+(,\d+)?\)|real|double precision|money|boolean|date|time|'
                         r'timestamp|timestamptz|timestamp with time zone|interval|uuid|json|jsonb|'
                         r'bytea|inet|macaddr|cidr|xml|serial|bigserial)(\[\])?$')


def pg_type(t):
    t = (t or 'text').strip().lower()
    if t.endswith('[]'):
        base = pg_type(t[:-2])
        return base + '[]' if not base.endswith('[]') else base
    if t.startswith('varchar') or t.startswith('character varying'):
        return t if '(' in t else 'character varying'
    if t.startswith('char') or t.startswith('character'):
        return t if '(' in t else 'character'
    if t in ('int', 'integer', 'int4', 'serial'):
        return 'integer'
    if t in ('bigint', 'int8', 'bigserial'):
        return 'bigint'
    if t in ('smallint', 'int2'):
        return 'smallint'
    if t in ('timestamptz', 'timestamp with time zone'):
        return 'timestamp with time zone'
    if t.startswith('timestamp'):
        return 'timestamp without time zone'
    if t.startswith('numeric') or t.startswith('decimal'):
        return t if '(' in t else 'numeric'
    if t in ('real', 'float4'):
        return 'real'
    if t in ('double precision', 'float8'):
        return 'double precision'
    if t in ('bool', 'boolean'):
        return 'boolean'
    if t == 'date':
        return 'date'
    if t == 'time':
        return 'time'
    if t == 'interval':
        return 'interval'
    if t == 'uuid':
        return 'uuid'
    if t == 'json':
        return 'json'
    if t == 'jsonb':
        return 'jsonb'
    if t == 'bytea':
        return 'bytea'
    if t == 'inet':
        return 'inet'
    if t == 'macaddr':
        return 'macaddr'
    if t == 'cidr':
        return 'cidr'
    if t == 'money':
        return 'money'
    if t == 'xml':
        return 'xml'
    return 'text'


def is_text(f):
    return pg_type(f.get('type')) in ('character varying', 'text')


def norm_type(t):
    """类型归一化：去空格并映射常见变体（timestamp without time zone → timestamp 等）"""
    t = (t or 'text').strip().lower().replace(' ', '')
    fix = {'timestampwithouttimezone': 'timestamp', 'timestampwithtimezone': 'timestamptz',
           'doubleprecision': 'double precision', 'charactervarying': 'varchar',
           'character': 'char', 'decimal': 'numeric'}
    return fix.get(t, t)


def _coltype(t):
    """json_to_recordset 列类型：白名单内的建表类型原样使用（保留 numeric(p,s)、varchar(n) 等精度），
    其余回退 pg_type 归一化。"""
    t0 = norm_type(t)
    if COL_TYPE_RE.match(t0):
        return t0
    return pg_type(t0)


def biz_fields(d):
    """业务字段（排除 isused/upid/idpath 等结构列）"""
    return biz_of(d['fields'], d['pk'])


def biz_of(fields, pk):
    """给定字段列表的业务字段（排除结构列与主键）"""
    return [f for f in fields if f['name'] not in ('isused', 'upid', 'idpath', 'idlevel', 'idcount', pk)]


def child_cfg(d):
    """parent_child 的子表配置：(child_table, child_pk, child_fk, child_fields, child_chinese)。
    parent_child 以父表为主模块（table/fields 即父表）；子表不生成独立函数，
    其内容包装成 JSON 随父表 add/edit/merge 一次性保存、由 query 一次性返回。"""
    if d['kind'] != 'parent_child' or not d.get('child_table'):
        return None
    return (d['child_table'], d['child_pk'], d['child_fk'],
            d['child_fields'], d['child_chinese'])


def _childjson_param(d):
    """父表 add/edit/merge 追加的子表 JSON 入参"""
    cc = child_cfg(d)
    return (', p_%sjson json' % cc[0]) if cc else ''


def _child_sync(d, idexpr):
    """子表整表 JSON 同步：按子表主键做增/删/改，不做“先删后插”——
    先删后插会重新生成子表主键，导致外部按子表主键关联的信息失效。规则：
      1) JSON 行带主键且库中存在 → 按主键 UPDATE（主键保持不变）；
      2) JSON 行主键为空或库中不存在 → INSERT（序列生成新主键）；
      3) 库中该父行下、主键不在提交集合中的行 → DELETE。
    idexpr 为父主键表达式（add 用 v_n，edit 用 p_<pk>，merge 用 v_id）。"""
    cc = child_cfg(d)
    if not cc:
        return ''
    s, ct, cpk, cfk, cfields = d['schema'], cc[0], cc[1], cc[2], cc[3]
    chi = d.get('child_hi', True)
    chs = d.get('child_hs', True)
    bf = biz_of(cfields, cpk)
    defs = ', '.join(['%s bigint' % cpk] + ['%s %s' % (f['name'], _coltype(f.get('type'))) for f in bf])
    cols = ', '.join(f['name'] for f in bf)
    sets = ', '.join('%s=x.%s' % (f['name'], f['name']) for f in bf)
    sell = ', '.join('x.' + f['name'] for f in bf)
    upd_extra = (', isused=1' if chi else '') + (", systemid=(v_check->'info'->>'systemid')::integer" if chs else '')
    icols = (',isused' if chi else '') + (',systemid' if chs else '')
    ivals = (",1" if chi else '') + (",(v_check->'info'->>'systemid')::integer" if chs else '')
    rs = "json_to_recordset(coalesce(p_{ct}json,'[]'::json)) as x({defs})".format(ct=ct, defs=defs)
    return (
        "  /* 子表按主键增删改：1) 删除该父行下、主键不在提交集合中的行\n"
        "     （delete 必须在 insert 之前，避免误删本次新插入、主键尚不在提交集合中的行） */\n"
        "  delete from {s}.{ct} t3 where t3.{cfk}={id} and not exists (select 1 from {rs} where x.{cpk}=t3.{cpk});\n"
        "  /* 2) 更新已存在行（按子表主键匹配，限当前父行，主键不变） */\n"
        "  update {s}.{ct} t set {sets}{upd_extra}\n"
        "  from {rs} where t.{cpk}=x.{cpk} and t.{cfk}={id} and x.{cpk} is not null;\n"
        "  /* 3) 插入新行（主键为空或库中不存在，由序列生成新主键） */\n"
        "  insert into {s}.{ct}({cfk},{cpk}{icols},{cols})\n"
        "  select {id},nextval('{s}.{ct}_{cpk}_seq'){ivals},{sell}\n"
        "  from {rs} where x.{cpk} is null or not exists (select 1 from {s}.{ct} t2 where t2.{cpk}=x.{cpk});\n"
    ).format(s=s, ct=ct, cpk=cpk, cfk=cfk, id=idexpr, cols=cols, sets=sets, sell=sell, rs=rs,
             upd_extra=upd_extra, icols=icols, ivals=ivals)


def _table_ddl(s, t, cn, pk, extra, fields, hi=True, hs=True):
    cols = ['  %s bigserial primary key' % pk] + extra
    for f in biz_of(fields, pk):
        cols.append('  %s %s' % (f['name'], f.get('type', 'text')))
    if hi:
        cols.append('  isused smallint default 1')
    if hs:
        cols.append('  systemid integer')
    return ('-- ======== %s（%s） ========\n'
            'CREATE TABLE IF NOT EXISTS %s.%s (\n%s\n);\n'
            'ALTER TABLE %s.%s OWNER TO gm;\n' % (t, cn, s, t, ',\n'.join(cols), s, t))


def tree_root_insert(d):
    """树表默认根节点（id=100，upid=0，idpath='100'，idlevel=1，idcount=0）。
    幂等：已存在 id=100 的记录时不重复插入。isused/systemid 按表实际结构附带。"""
    s, t, pk = d['schema'], d['table'], d['pk']
    cols = [pk, 'upid', 'idpath', 'idlevel', 'idcount']
    vals = ["100", "0", "'100'", "1", "0"]
    if d.get('has_isused', True):
        cols.append('isused'); vals.append('1')
    if d.get('has_sysid', True):
        cols.append('systemid'); vals.append(str(int(d.get('systemid') or 100)))
    return ('-- 默认根节点（id=100，幂等去重）\n'
            'INSERT INTO %s.%s (%s)\n'
            'SELECT %s\n'
            'WHERE NOT EXISTS (SELECT 1 FROM %s.%s WHERE %s=100);\n'
            % (s, t, ','.join(cols), ','.join(vals), s, t, pk))


def gen_create_table(d):
    s, t, pk = d['schema'], d['table'], d['pk']
    cc = child_cfg(d)
    hi, hs = d.get('has_isused', True), d.get('has_sysid', True)
    if cc:
        ct, cpk, cfk, cfields, ccn = cc
        # 父子表：父表 + 子表（子表带指向父主键的外键列 child_fk，默认 upid）
        return '\n'.join([_table_ddl(s, t, d['chinese'], pk, [], d['fields'], hi, hs),
                          _table_ddl(s, ct, ccn, cpk, ['  %s bigint default 0' % cfk], cfields, hi, hs)])
    extra = []
    if d['kind'] == 'tree':
        extra = ['  upid bigint default 0',
                 '  idpath character varying(255)', '  idlevel integer', '  idcount integer']
    ddl = _table_ddl(s, t, d['chinese'], pk, extra, d['fields'], hi, hs)
    if d['kind'] == 'tree':
        ddl += tree_root_insert(d)      # 树表默认根节点（id=100，幂等）
    return ddl


def where_clause(d, fks=None, alias='t'):
    """query 的 where 条件：文本 position 模糊，数值/时间相等；数组外键列按文本比较"""
    fkm = {k['fkcol']: k for k in (fks or [])}
    conds = []
    for f in biz_fields(d):
        n = f['name']
        if n in fkm and fkm[n]['isarray']:
            conds.append('(%s is null or array_to_string(%s.%s, %s)=%s)' % ('p_' + n, alias, n, "','", 'p_' + n))
        elif is_text(f):
            conds.append('(%s is null or position(%s in %s.%s)>0)' % ('p_' + n, 'p_' + n, alias, n))
        else:
            conds.append('(%s is null or %s.%s=%s)' % ('p_' + n, alias, n, 'p_' + n))
    if d.get('has_isused', True):
        conds.append('(p_isused is null or %s.isused=p_isused)' % alias)
    return ' and '.join(conds)


def name_field(d):
    nf = d.get('name_field')
    if nf:
        return nf
    bf = biz_fields(d)
    for f in bf:
        if 'name' in f['name']:
            return f['name']
    for f in bf:
        if is_text(f):
            return f['name']
    return bf[0]['name'] if bf else d['pk']


def get_fks(d, qfunc=None):
    """读取 public.fk_meta 中本表(fk_schema,fk_table)的外键引用配置（isused=1）。
       返回列表，元素含：
       ms/mt/mc（主表三方）、multi（可多选）、fkcol（外键列）、disps（主表显示字段）、
       module（主表前端模块 id）、istree（主表是否树表）、mdb/fschema（主库与生成 SQL 使用的
       schema：分库时为 fdwext）、pktype（主表主键类型）、etype（外键元素类型=主键类型）、
       isarray（外键列是否数组，多选时为真）、coltype（外键列应有类型）。"""
    if qfunc is None:
        def qfunc(sql):
            env = dict(os.environ, PGPASSWORD=PG['password'])
            r = subprocess.run(['psql', '-h', PG['host'], '-U', PG['user'], '-d', PG['db'],
                                '-tAc', sql], env=env, capture_output=True, text=True, timeout=15)
            return (r.stdout or '').strip()
    try:
        import json as _json
        rows = qfunc("select coalesce(json_agg(t),'[]'::json)::text from (select master_schema,master_table,"
                     "master_column,master_multi,fk_column,display_columns,master_module,master_db from public.fk_meta "
                     "where isused=1 and fk_schema='%s' and fk_table='%s' order by id) t" % (d['schema'], d['table']))
        arr = _json.loads(rows or '[]')
    except Exception as e:
        print('fk_meta 读取失败，忽略外键：', e)
        return []
    fks = []
    for r in arr:
        disps = [x.strip() for x in str(r.get('display_columns') or '').split(',') if x.strip()]
        if not disps:
            continue
        mdb = str(r.get('master_db') or 'local')
        fs = 'fdwext' if mdb not in ('', 'local') else r['master_schema']
        dcn = {}
        try:
            cnrows = qfunc("select coalesce(json_agg(t),'[]'::json)::text from (select c.column_name,"
                           "coalesce(col_description((quote_ident(c.table_schema)||'.'||quote_ident(c.table_name))::regclass,c.ordinal_position),'') cn "
                           "from information_schema.columns c where c.table_schema='%s' and c.table_name='%s') t"
                           % (r['master_schema'], r['master_table']))
            for c in _json.loads(cnrows or '[]'):
                dcn[c['column_name']] = c['cn']
        except Exception:
            pass
        istree = False
        try:
            istree = qfunc("select count(*) from information_schema.columns where table_schema='%s' "
                           "and table_name='%s' and column_name='idpath'"
                           % (r['master_schema'], r['master_table'])) == '1'
        except Exception:
            pass
        # 主表主键类型（外键元素类型 etype 的权威来源）
        pktype = 'bigint'
        try:
            trow = qfunc("select coalesce(json_agg(t),'[]'::json)::text from (select c.data_type,"
                         "c.character_maximum_length len,c.numeric_precision prec,c.numeric_scale scale "
                         "from information_schema.columns c where c.table_schema='%s' and c.table_name='%s' "
                         "and c.column_name='%s') t" % (r['master_schema'], r['master_table'], r['master_column']))
            tl = _json.loads(trow or '[]')
            if tl:
                dt = tl[0]['data_type']
                if dt == 'character varying' and tl[0].get('len'):
                    pktype = 'varchar(%d)' % tl[0]['len']
                elif dt == 'numeric' and tl[0].get('prec'):
                    pktype = 'numeric(%s,%s)' % (tl[0]['prec'], tl[0]['scale'])
                elif dt in ('integer', 'bigint', 'smallint', 'uuid', 'text'):
                    pktype = dt
                elif dt == 'character':
                    pktype = 'char(%s)' % (tl[0].get('len') or 1)
        except Exception:
            pass
        # 建模定义中的外键列类型（前端按外键要求回填，如 bigint[]）
        ftype = ''
        for f in (d.get('fields') or []):
            if f.get('name') == r['fk_column']:
                ftype = str(f.get('type') or '')
                break
        isarray = bool(r.get('master_multi')) or ftype.endswith('[]')
        etype = (ftype[:-2].strip() if isarray else ftype) or pktype
        coltype = (etype + '[]') if isarray else etype
        fks.append({'ms': r['master_schema'], 'mt': r['master_table'], 'mc': r['master_column'],
                    'multi': int(r.get('master_multi') or 0), 'fkcol': r['fk_column'],
                    'disps': [{'name': x, 'cn': dcn.get(x) or x} for x in disps],
                    # 主表前端模块 id：未登记时按项目约定默认为主表表名（模块 id=表名），
                    # 否则生成的表单字段缺 tp/lp/multi 标记，前端外键选择器/多选/回显失效
                    'module': r.get('master_module') or r['master_table'], 'istree': istree,
                    'mdb': mdb, 'fschema': fs, 'pktype': pktype,
                    'etype': etype, 'coltype': coltype, 'isarray': isarray})
    return fks


def _fk_checks(d, fks):
    """外键存在性校验（add/edit/merge 前置）：单值直接查主表；多选拆逗号逐值查。
       校验失败返回通用码 100039（关联数据不存在！）。"""
    out = ''
    for fk in (fks or []):
        c = fk['fkcol']
        if fk['multi']:
            out += ("  if p_{c} is not null and p_{c}::text<>'' then\n"
                    "    select count(*) into v_c from (select trim(x) xv from unnest(string_to_array(p_{c}::text,',')) x) s\n"
                    "      where not exists (select 1 from {fs}.{mt} m where m.{mc}::text=s.xv);\n"
                    "    if v_c>0 then return gm.returnjson(100039); end if;--关联数据不存在\n"
                    "  end if;\n").format(c=c, fs=fk['fschema'], mt=fk['mt'], mc=fk['mc'])
        else:
            out += ("  if p_{c} is not null then\n"
                    "    select count(*) into v_c from {fs}.{mt} where {mc}::text=p_{c}::text;\n"
                    "    if v_c=0 then return gm.returnjson(100039); end if;--关联数据不存在\n"
                    "  end if;\n").format(c=c, fs=fk['fschema'], mt=fk['mt'], mc=fk['mc'])
    return out


def _fk_param_type(f, fkm):
    """外键列的函数参数类型：数组/多选列一律 varchar（前端传逗号分隔文本），其余按列类型"""
    fk = fkm.get(f['name'])
    if fk and (fk['isarray'] or fk['multi']):
        return 'varchar'
    return pg_type(f.get('type', 'text'))


def _fk_val(f, fkm):
    """insert/update 中外键列的取值表达式：数组列把逗号文本参数转为数组，其余原样"""
    fk = fkm.get(f['name'])
    if fk and fk['isarray']:
        return ("(SELECT coalesce(array_agg(x::{et} ORDER BY x), ARRAY[]::{et}[]) FROM "
                "unnest(string_to_array(p_{c}::text, ',')) x WHERE trim(x) <> '')").format(et=fk['etype'], c=fk['fkcol'])
    return 'p_' + f['name']


def select_list(d, fks=None):
    s, t, pk = d['schema'], d['table'], d['pk']
    items = ['t.*']
    cc = child_cfg(d)
    if cc:
        ct, cpk, cfk = cc[0], cc[1], cc[2]
        # 查询一次性返回该父行下全部子表行（JSON 数组），前端明细直接消费
        items.append('(select array_to_json(array_agg(row_to_json(c))) from '
                     '(select c1.* from {s}.{ct} c1 where c1.{cfk}=t.{pk} order by c1.{cpk}) c) as {ct}json'
                     .format(s=s, ct=ct, cfk=cfk, pk=pk, cpk=cpk))
    if d['kind'] == 'tree':
        items += ['tt.%s as upname' % name_field(d)]
    for i, fk in enumerate(fks or []):
        if fk['multi'] or fk['isarray']:
            # 数组列 ::text 是 {a,b} 形式，需去掉花括号再拆分（'{{}}' 为 format 转义的字面花括号）
            sub = ("(select string_agg(m.{d}::text, ',' order by m.{mc}::text) from {fs}.{mt} m"
                   " where m.{mc}::text = any(string_to_array(trim(coalesce(t.{c}::text, ''), '{{}}'), ',')))")
            for dsp in fk['disps']:
                items.append(sub.format(d=dsp['name'], mc=fk['mc'], fs=fk['fschema'], mt=fk['mt'],
                                        c=fk['fkcol']) + ' as %s_%s' % (fk['fkcol'], dsp['name']))
        else:
            for dsp in fk['disps']:
                items.append('fj%d.%s as %s_%s' % (i, dsp['name'], fk['fkcol'], dsp['name']))
    return ', '.join(items)


def from_clause(d, fks=None):
    s, t, pk = d['schema'], d['table'], d['pk']
    frm = ' from %s.%s t' % (s, t)
    if d['kind'] == 'tree':
        frm += ' left join %s.%s tt on t.upid=tt.%s' % (s, t, pk)
    for i, fk in enumerate(fks or []):
        if fk['multi'] or fk['isarray']:
            continue  # 多选/数组外键的显示用 select_list 中的标量子查询聚合，不展开行
        # ::text 比较：避免外键列与主表主键类型不一致（如 varchar vs integer）导致 join 报错
        frm += (' left join ' + fk['fschema'] + '.' + fk['mt'] + ' fj' + str(i)
                + ' on t.' + fk['fkcol'] + '::text=fj' + str(i) + '.' + fk['mc'] + '::text')
    return frm


def tree_recalc(d):
    """全表 idpath/idlevel/idcount 重算（参照 sysorg）；isused 过滤与 systemid 字段
    按表实际结构（has_isused/has_sysid）条件化——建模树表可以没有这两列"""
    s, t, pk = d['schema'], d['table'], d['pk']
    hi = d.get('has_isused', True)
    hs = d.get('has_sysid', True)
    iu1 = ' where isused<>0' if hi else ''
    iu2 = ' where isused<>0' if hi else ''
    ssys = ',t1.systemid' if hs else ''
    jsys = ' and t2.systemid=t1.systemid' if hs else ''
    ccol = ',systemid' if hs else ''
    grp = ' group by upid,systemid' if hs else ' group by upid'
    return ("  with recursive tree as\n"
            "  ( select %(pk)s,upid,1 as idlevel,%(pk)s::varchar as idpath%(ssys)s from %(s)s.%(t)s t1 where coalesce(upid,0)=0\n"
            "  union\n"
            "  select t1.%(pk)s,t1.upid,t2.idlevel+1,t2.idpath||'.'||t1.%(pk)s%(ssys)s from %(s)s.%(t)s t1 inner join tree t2 on t2.%(pk)s=t1.upid%(jsys)s%(iu1)s)\n"
            "  update %(s)s.%(t)s t set idpath=t1.idpath,idlevel=t1.idlevel,idcount=coalesce(cc,0) from tree t1 left join (select upid,count(*) cc%(ccol)s from %(s)s.%(t)s%(iu2)s%(grp)s) t2 on t1.%(pk)s=t2.upid\n"
            "  where t.%(pk)s=t1.sysorgid;" % {'s': s, 't': t, 'pk': pk, 'ssys': ssys, 'jsys': jsys,
                                                'iu1': iu1, 'iu2': iu2, 'ccol': ccol, 'grp': grp}).replace('t1.sysorgid', 't1.' + pk)


def fn_head(d, op, params):
    s = d['schema']
    fp = d.get('fp') or d['table']
    return ('CREATE OR REPLACE FUNCTION %s.%s_%s(%s) RETURNS json\n'
            '    LANGUAGE plpgsql SECURITY DEFINER\n    AS $$\n') % (s, fp, op, params)


def gen_query(d, fks=None):
    s, t, pk = d['schema'], d['table'], d['pk']
    bf = biz_fields(d)
    fkm = {k['fkcol']: k for k in (fks or [])}
    params = 'p_token character varying, p_rows integer, p_page integer'
    params += ''.join(', p_%s %s' % (f['name'], _fk_param_type(f, fkm)) for f in bf)
    params += ', p_isused integer'
    w = where_clause(d, fks)
    sel, frm, order = select_list(d, fks), from_clause(d, fks), 't.' + pk
    if d['kind'] == 'tree':
        order = 't.idpath'
    sql = fn_head(d, 'query', params)
    sql += ("declare v_c integer;v_return json;v_check json;\nbegin\n"
            "  v_check:=gm.check_login(p_token,{qa});\n"
            "  if v_check->>'errorcode'<>'0' then return v_check; end if;\n"
            "  if p_rows is null then\n"
            "    select array_to_json(array_agg(row_to_json(t))) into v_return from (select {sel}{frm} where {w} order by {ord}) t;\n"
            "    v_c:=coalesce(json_array_length(v_return),0);\n"
            "  else\n"
            "    select count(*) into v_c{frm} where {w};\n"
            "    select array_to_json(array_agg(row_to_json(t))) into v_return from (select {sel}{frm} where {w} order by {ord} limit greatest(p_rows,1) offset greatest(least((p_page-1)*p_rows,(v_c/p_rows-1+abs(v_c % p_rows))*p_rows),0)) t;\n"
            "  end if;\n"
            "  return gm.returnjson(0,json_build_object('total',v_c,'rows',v_return));\n"
            "end;\n$$;\n").format(qa=d['actions']['query'], sel=sel, frm=frm, w=w, ord=order)
    return sql, params


def _upid_param(d):
    return ', p_upid bigint' if d['kind'] == 'tree' else ''


def _parent_check(d):
    if d['kind'] != 'tree':
        return ''
    s, t, pk = d['schema'], d['table'], d['pk']
    iu = ' and isused<>0' if d.get('has_isused', True) else ''
    return ("  if p_upid is not null and p_upid<>0 then\n"
            "    select count(*) into v_c from {s}.{t} where {pk}=p_upid{iu};\n"
            "    if v_c=0 then return gm.returnjson(100014);end if;--上级不存在（upid=0 表示根节点，不校验）\n"
            "  end if;\n").format(s=s, t=t, pk=pk, iu=iu)


def _loop_check(d, idvar='v_idpath'):
    """树形：上级不能是自身或其下级（循环定义）"""
    if d['kind'] != 'tree':
        return ''
    s, t, pk = d['schema'], d['table'], d['pk']
    iu = ' and isused<>0' if d.get('has_isused', True) else ''
    return ("  if p_upid=p_{pk} then return gm.returnjson(100017);end if;--上级不能是自身\n"
            "  select idpath into {v} from {s}.{t} where {pk}=p_upid{iu};\n"
            "  if position('.'||p_{pk}||'.' in '.'||coalesce({v},'')||'.')>0 then return gm.returnjson(100017);end if;--不能循环定义\n").format(v=idvar, s=s, t=t, pk=pk, iu=iu)


def gen_add(d, fks=None):
    s, t, pk = d['schema'], d['table'], d['pk']
    bf = biz_fields(d)
    fkm = {k['fkcol']: k for k in (fks or [])}
    params = 'p_token character varying' + _upid_param(d)
    if d['kind'] == 'tree':
        params += ', p_%s bigint' % pk
    params += ''.join(', p_%s %s' % (f['name'], _fk_param_type(f, fkm)) for f in bf)
    params += _childjson_param(d)
    cols, vals = [], []
    if d.get('has_isused', True):
        cols.append('isused'); vals.append('1')
    if d.get('has_sysid', True):
        cols.append('systemid'); vals.append("(v_check->'info'->>'systemid')::integer")
    if d['kind'] == 'tree':
        cols.insert(0, 'upid'); vals.insert(0, 'coalesce(p_upid,0)')
    if d['kind'] == 'tree':
        cols.insert(0, pk); vals.insert(0, 'v_id')
    for f in bf:
        cols.append(f['name'])
        vals.append(_fk_val(f, fkm))
    seq = ''
    if d['kind'] == 'tree':
        seq = ("  if p_{pk} is null then\n"
               "    v_c:=1;loop exit when (v_c=0);\n"
               "      v_id:=nextval('{s}.{t}_{pk}_seq');\n"
               "      select count(*) into v_c from {s}.{t} where {pk}=v_id;\n"
               "    end loop;\n"
               "  else\n"
               "    select count(*) into v_c from {s}.{t} where {pk}=p_{pk};\n"
               "    if v_c>0 then return gm.returnjson(100018); end if;--主键已存在\n"
               "    v_id:=p_{pk};\n"
               "  end if;\n").format(s=s, t=t, pk=pk)
    sql = fn_head(d, 'add', params)
    sql += ("declare v_c int;{idvar} int;v_idpath varchar;v_check json;\nbegin\n"
            "  v_check:=gm.check_login(p_token,{a});\n"
            "  if v_check->>'errorcode'<>'0' then return v_check; end if;\n").format(idvar='v_id' if d['kind'] == 'tree' else 'v_n', a=d['actions']['add'])
    sql += _fk_checks(d, fks) + _parent_check(d) + _loop_check(d) + seq
    sql += ("  insert into {s}.{t}({cols}) values({vals}) returning {pk} into {idvar};\n").format(
        s=s, t=t, cols=','.join(cols), vals=','.join(vals), pk=pk, idvar='v_id' if d['kind'] == 'tree' else 'v_n')
    if d['kind'] == 'tree':
        sql += tree_recalc(d) + '\n'
    # 父子表：子表整表 JSON 按主键增删改（新增父行下 update/delete 空转，等价纯插入）
    sql += _child_sync(d, 'v_n')
    sql += "  return gm.returnjson(0,json_build_object('id',{}));\nend;\n$$;\n".format('v_id' if d['kind'] == 'tree' else 'v_n')
    return sql, params


def gen_edit(d, fks=None):
    s, t, pk = d['schema'], d['table'], d['pk']
    bf = biz_fields(d)
    fkm = {k['fkcol']: k for k in (fks or [])}
    params = 'p_token character varying, p_%s bigint' % pk + _upid_param(d)
    params += ''.join(', p_%s %s' % (f['name'], _fk_param_type(f, fkm)) for f in bf)
    params += _childjson_param(d)
    sets = ['%s=%s' % (f['name'], _fk_val(f, fkm)) for f in bf]
    if d['kind'] == 'tree':
        sets.insert(0, 'upid=coalesce(p_upid,upid)')
    sql = fn_head(d, 'edit', params)
    sql += ("declare v_c int;v_idpath varchar;v_check json;\nbegin\n"
            "  v_check:=gm.check_login(p_token,{a});\n"
            "  if v_check->>'errorcode'<>'0' then return v_check; end if;\n"
            "  select count(*) into v_c from {s}.{t} where {pk}=p_{pk};\n"
            "  if v_c=0 then return gm.returnjson(100018); end if;--记录不存在\n").format(a=d['actions']['edit'], s=s, t=t, pk=pk)
    sql += _fk_checks(d, fks) + _parent_check(d)
    sql += _loop_check(d)
    sql += "  update {s}.{t} set {sets} where {pk}=p_{pk};\n".format(s=s, t=t, sets=','.join(sets), pk=pk)
    if d['kind'] == 'tree':
        sql += tree_recalc(d) + '\n'
    # 父子表：子表整表 JSON 按主键增删改（不整表先删后插，保持子表主键稳定）
    sql += _child_sync(d, 'p_' + pk)
    sql += '  return gm.returnjson(0);\nend;\n$$;\n'
    return sql, params


def gen_merge(d, fks=None):
    s, t, pk = d['schema'], d['table'], d['pk']
    bf = biz_fields(d)
    fkm = {k['fkcol']: k for k in (fks or [])}
    params = 'p_token character varying' + _upid_param(d) + ', p_%s bigint' % pk
    params += ''.join(', p_%s %s' % (f['name'], _fk_param_type(f, fkm)) for f in bf)
    params += _childjson_param(d)
    sets = ['%s=%s' % (f['name'], _fk_val(f, fkm)) for f in bf]
    if d['kind'] == 'tree':
        sets.insert(0, 'upid=coalesce(p_upid,upid)')
    sets.append('systemid=(v_check->\'info\'->>\'systemid\')::integer')
    icols = ['isused', 'systemid']
    ivals = ['1', "(v_check->'info'->>'systemid')::integer"]
    if d['kind'] == 'tree':
        icols.insert(0, 'upid'); ivals.insert(0, 'coalesce(p_upid,0)')
    if d['kind'] == 'tree':
        icols.insert(0, pk); ivals.insert(0, 'v_id')
    for f in bf:
        icols.append(f['name'])
        ivals.append(_fk_val(f, fkm))
    seq = ''
    if d['kind'] == 'tree':
        seq = ("  if p_{pk} is null then\n"
               "    v_c:=1;loop exit when (v_c=0);\n"
               "      v_id:=nextval('{s}.{t}_{pk}_seq');\n"
               "      select count(*) into v_c from {s}.{t} where {pk}=v_id;\n"
               "    end loop;\n"
               "  else\n"
               "    v_id:=p_{pk};\n"
               "  end if;\n").format(s=s, t=t, pk=pk)
    else:
        seq = "  v_id:=p_{pk};\n".format(pk=pk)
    sql = fn_head(d, 'merge', params)
    sql += ("declare v_c int;v_id int;v_idpath varchar;v_check json;\nbegin\n"
            "  v_check:=gm.check_login(p_token,{a});\n"
            "  if v_check->>'errorcode'<>'0' then return v_check; end if;\n").format(a=d['actions']['merge'])
    sql += _fk_checks(d, fks) + _parent_check(d)
    sql += _loop_check(d) + seq
    if d['kind'] == 'tree':
        sql += ("  with \"te\" as (update {s}.{t} set {sets} where {pk}=p_{pk} returning *)\n"
                "  insert into {s}.{t} ({cols}) select {vals} where (select count(*) from te)=0;\n").format(
            s=s, t=t, sets=','.join(sets), pk=pk, cols=','.join(icols), vals=','.join(ivals))
        sql += tree_recalc(d) + '\n'
    else:
        # single/parent_child：先 update，未命中(FOUND=false)再 insert 并 returning 主键，
        # 修复“传入不存在 id 时 merge 返回的 id 是调用方无效值”的问题
        sql += ("  update {s}.{t} set {sets} where {pk}=p_{pk};\n"
                "  if found then\n"
                "    v_id:=p_{pk};\n"
                "  else\n"
                "    insert into {s}.{t} ({cols}) values ({ivals}) returning {pk} into v_id;\n"
                "  end if;\n").format(
            s=s, t=t, sets=','.join(sets), pk=pk, cols=','.join(icols), ivals=','.join(ivals))
    # 父子表：子表整表 JSON 按主键增删改（不整表先删后插，保持子表主键稳定）
    sql += _child_sync(d, 'v_id')
    sql += "  return gm.returnjson(0,json_build_object('id',v_id));\nend;\n$$;\n"
    return sql, params


def gen_del(d, op):
    s, t, pk = d['schema'], d['table'], d['pk']
    has_isused = d.get('has_isused', True)
    if not has_isused and op == 'undel':
        return None, None                            # 无 isused：无恢复语义，不生成 undel
    sql = fn_head(d, op, 'p_token character varying, p_%s bigint' % pk)
    sql += ("declare v_c int;v_check json;\nbegin\n"
            "  v_check:=gm.check_login(p_token,{a});\n"
            "  if v_check->>'errorcode'<>'0' then return v_check; end if;\n"
            "  select count(*) into v_c from {s}.{t} where {pk}=p_{pk};\n"
            "  if v_c=0 then return gm.returnjson(100018); end if;--记录不存在\n").format(a=d['actions'][op], s=s, t=t, pk=pk)
    if op == 'del' and d['kind'] == 'tree':
        iu = ' and isused<>0' if has_isused else ''
        sql += ("  select count(*) into v_c from {s}.{t} where upid=p_{pk}{iu};\n"
                "  if v_c>0 then return gm.returnjson(100016);end if;--有下级不能删除\n").format(s=s, t=t, pk=pk, iu=iu)
    if op == 'del':
        cc = child_cfg(d)
        if cc:
            # 父子表：有子记录不能删除（对齐树形防护，错误码 100016）；子表 isused 按实际结构条件化
            chas = any((f.get('name') == 'isused') for f in (d.get('child_fields') or []))
            ciu = ' and isused<>0' if chas else ''
            sql += ("  if exists(select 1 from {s}.{ct} where {cfk}=p_{pk}{ciu}) "
                    "then return gm.returnjson(100016);end if;--有子记录不能删除\n"
                    ).format(s=s, ct=cc[0], cfk=cc[2], pk=pk, ciu=ciu)
    if has_isused:
        sql += "  update {s}.{t} set isused={f} where {pk}=p_{pk};\n".format(s=s, t=t, f='0' if op == 'del' else '1', pk=pk)
    else:
        # 无 isused：物理删除
        sql += "  delete from {s}.{t} where {pk}=p_{pk};\n".format(s=s, t=t, pk=pk)
    if d['kind'] == 'tree':
        sql += tree_recalc(d) + '\n'
    sql += '  return gm.returnjson(0);\nend;\n$$;\n'
    return sql, 'p_token character varying, p_%s bigint' % pk


def gen_comment(d, op, params):
    s, t, pk = d['schema'], d['table'], d['pk']
    fp = d.get('fp') or t
    cc = child_cfg(d)
    lines = ['%s%s' % (VERBS[op], d['chinese'])]
    if op in ('add', 'edit', 'merge'):
        if d['kind'] == 'tree':
            lines.append('upid:上级id（%s）' % fp)
        for f in biz_fields(d):
            lines.append('%s:%s' % (f['name'], f.get('cn', '')))
        if cc:
            lines.append('%sjson:子表%s整表JSON数组（一次性覆盖保存：先按父主键删旧再插入）' % (cc[0], cc[4]))
    elif op == 'query':
        if d.get('has_isused', True):
            lines.append('参数：token/rows/page + 各业务字段（文本模糊、数值精确）+ isused；rows 为空返回全量')
        else:
            lines.append('参数：token/rows/page + 各业务字段（文本模糊、数值精确）；rows 为空返回全量')
        if cc:
            lines.append('返回 rows 每行含 %sjson：该行全部子表%s（JSON 数组，一次性返回）' % (cc[0], cc[4]))
    else:
        lines.append('%s:%s' % (pk, '主键'))
    tail = '{"errorcode":0,"message":"执行成功！"}'
    if op in ('add', 'merge'):
        tail = '{"errorcode":0,"message":"执行成功！","info":{"id":id}}'
    elif op == 'query':
        tail = '{"errorcode":0,"message":"执行成功！","info":{"total":n,"rows":[...]}}'
    lines.append('返回：' + tail)
    body = '\n'.join(lines).replace("'", "''")
    return 'COMMENT ON FUNCTION %s.%s_%s(%s) IS \'%s\';\n\n' % (s, fp, op, params, body)



def gen_sysaction(d):
    g, m = d['group'], d['menu_action']
    s = d['schema']
    fp = d.get('fp') or d['table']
    # 挂载父节点的 idpath：默认 '100.<分组>'（分组节点位于第 2 层）；
    # admin 权限树可选任意节点，parent_idpath 由管理台查询 sysaction 后传入
    base = d.get('parent_idpath') or ('100.%s' % g)
    mlvl = base.count('.') + 2
    rows = [(None, (1, '%s.%s' % (base, m), mlvl, 6, int(g), None, d['systemid'], m, d['chinese'], None, None))]
    for op, act in d['actions'].items():
        rows.append((op, (1, '%s.%s.%s' % (base, m, act), mlvl + 1, 0, m, None, d['systemid'], act, VERBS[op] + d['chinese'], act, None)))
    sql = '-- 权限节点（菜单 %s：%s，挂于 %s 之下）\n' % (m, d['chinese'], g)
    for op, r in rows:
        vals = ', '.join('NULL' if v is None else (str(v) if not isinstance(v, str) else "'%s'" % v) for v in r)
        # ON CONFLICT DO UPDATE：重复发布时节点跟随本次挂载点/名称自愈更新，
        # 不会残留旧挂载位置的孤儿节点（此前 DO NOTHING 会留下两套节点）
        sql += ('INSERT INTO sysinfo.sysaction (isused,idpath,idlevel,idcount,upid,params,systemid,sysactionid,sysactionname,actionid,isdefault)\n'
                'VALUES (%s)\n'
                'ON CONFLICT (sysactionid) DO UPDATE SET isused=1, upid=EXCLUDED.upid, idpath=EXCLUDED.idpath,\n'
                '  idlevel=EXCLUDED.idlevel, idcount=EXCLUDED.idcount, sysactionname=EXCLUDED.sysactionname,\n'
                '  actionid=EXCLUDED.actionid, systemid=EXCLUDED.systemid;\n' % vals)
        if r[9] is not None:
            # 操作节点同步写入 actions（具体权限表），actionid 与 sysaction.actionid 共用；
            # 按 actionurl 判重（跳过已存在的相同接口权限），保证重复发布幂等
            sql += ("INSERT INTO sysinfo.actions (isused, actionid, actionname, actionurl)\n"
                    "SELECT 1, %d, '%s', '%s/%s_%s' WHERE NOT EXISTS "
                    "(SELECT 1 FROM sysinfo.actions WHERE actionid=%d OR actionurl='%s/%s_%s');\n"
                    % (r[9], VERBS[op] + d['chinese'], s, fp, op, r[9], s, fp, op))
    return sql + '\n'


def gen_sql(d, fks=None):
    s, t = d['schema'], d['table']
    fp = d.get('fp') or t
    out = ['\n-- ==================== [%s] %s.%s：由 tools/gen_module.py 生成 %s ====================\n'
           % (d['kind'], s, t, datetime.date.today().isoformat())]
    if d.get('existing'):
        out.append('-- 父子表联动模块：父表 %s.%s 与子表 %s.%s 均已存在，此处仅生成联动函数\n'
                   '--（父表/子表的单表 CRUD 请各自单独生成，函数名以 %s 为前缀与之区分）\n'
                   % (s, t, s, d.get('child_table') or '', fp))
    else:
        out.append(gen_create_table(d))
    gens = {'query': gen_query, 'add': gen_add, 'edit': gen_edit, 'merge': gen_merge}
    # 父子表联动模块固定 5 个操作（query/add/edit/merge/del），无 undel（恢复由单表模块负责）
    ops = ('query', 'add', 'edit', 'merge', 'del') if d.get('existing') else ('query', 'add', 'edit', 'merge', 'del', 'undel')
    for op in ops:
        if op in gens:
            sql, params = gens[op](d, fks)
        else:
            sql, params = gen_del(d, op)
        if sql is None:
            continue                                  # 无 isused 的表不生成 undel
        out.append('--\n-- Name: %s_%s(%s); Type: FUNCTION; Schema: %s; Owner: gm\n--\n\n' % (fp, op, params, s))
        out.append(sql)
        out.append('ALTER FUNCTION %s.%s_%s(%s) OWNER TO gm;\n\n' % (s, fp, op, params))
        out.append(gen_comment(d, op, params))
    out.append(gen_sysaction(d))
    return '\n'.join(out)


def gen_modules_js(d, fks=None):
    s, t, pk = d['schema'], d['table'], d['pk']
    fp = d.get('fp') or t
    cc = child_cfg(d)
    cols, fields = ['{f:%r,t:%r}' % (pk, '主键')], []
    if d['kind'] == 'tree':
        cols.append('{f:%r,t:%r}' % ('upid', '上级'))
        cols.append('{f:%r,t:%r}' % ('upname', '上级名称'))
    for f in biz_fields(d):
        c = '{f:%r,t:%r}' % (f['name'], f.get('cn') or f['name'])
        if f.get('wide'): c = c[:-1] + ',w:1}'
        if f.get('flag'): c = c[:-1] + ',b:1}'
        cols.append(c)
        fk = next((k for k in (fks or []) if k['fkcol'] == f['name']), None)
        if fk:
            fcn = f.get('cn') or f['name']
            for dsp in fk['disps']:
                cols.append('{f:%r,t:%r,w:1}' % (f['name'] + '_' + dsp['name'], fcn + dsp['cn']))
        attrs = {'f': f['name'], 't': f.get('cn') or f['name']}
        if f['name'] == pk or f.get('required'): attrs['r'] = 1
        if f.get('flag'): attrs['sw'] = 1
        if f.get('type'): attrs['ty'] = f['type']   # 数据库类型：前端提交前做类型校验
        if fk and fk.get('disps'): attrs['disp'] = ','.join(d['name'] for d in fk['disps'])   # 外键显示列：表单回显与列表选择器弹窗列（编号+各显示列）
        if fk and fk['module']:
            attrs['tp'] = fk['module']
            if not fk['istree']: attrs['lp'] = 1
            if fk['multi']: attrs['multi'] = 1
        fields.append('{' + ','.join('%s:%r' % (k, v) for k, v in attrs.items()) + '}')
    tree = '1' if d['kind'] == 'tree' else 'null'
    if d['kind'] == 'tree':
        fields.insert(1, "{f:'upid',tp:'%s'}" % t)
    dt = "'%s'" % d['dt'] if d.get('dt') else 'null'
    undel = 'null' if (not d.get('has_isused', True) or d.get('existing')) else d['actions'].get('undel')
    line = ("/* %s（%s）：由 tools/gen_module.py 生成 */\n"
            "P('%s','%s','%s',%s,'%s','%s/%s','%s',\n"
            "  [%s],\n  [%s],\n  {add:%s,edit:%s,merge:%s,del:%s,undel:%s},%s,%s);\n"
            % (fp, d['chinese'], fp, d['chinese'], d['group'], d['menu_action'], d['icon'], s, fp, pk,
               ','.join(cols), ','.join(fields), d['actions']['add'], d['actions']['edit'],
               d['actions']['merge'], d['actions']['del'], undel, dt, tree))
    if cc:
        ct, cpk, cfk, cfields, ccn = cc
        # 父表“明细”挂载子表：整表 JSON 一次性提交给父表 _edit（不调用子表独立接口），
        # 查询由父表 _query 返回 <child>json。app.js 启动时把 SUBTABLECFG 合入 SUBTABLE。
        sfields = []
        for f in biz_of(cfields, cpk):
            fd = '{f:%r' % f['name']
            if f.get('required'): fd += ',r:1'
            if f.get('flag'): fd += ',sw:1'
            if not f.get('flag') and not is_text(f): fd += ',t:"num"'
            fd += '}'
            sfields.append(fd)
        ccols = []
        for f in biz_of(cfields, cpk):
            c = '{f:%r}' % f['name']
            if f.get('flag'): c = c[:-1] + ',b:1}'
            ccols.append(c)
        line += ("/* 父子表：父表 %s 的“明细”挂载子表 %s——整表 JSON 一次性提交给 %s_edit，\n"
                 "   查询由 %s_query 返回 %sjson；不生成/不调用子表独立接口 */\n"
                 "SUBTABLECFG.push({key:'%s',t:'%s',fk:'%s',pk:'%s',jsonKey:'%sjson',"
                 "editApi:'%s/%s_edit',perm:{edit:%s},fields:[%s],cols:[%s],extra:[]});\n"
                 % (t, ct, fp, fp, ct, ct, ccn, pk, cpk, ct, s, fp,
                    d['actions']['edit'], ','.join(sfields), ','.join(ccols)))
    return line


def patch_readme(d, text):
    text = re.sub(r'(\d+) 函数', lambda m: str(int(m.group(1)) + 6) + ' 函数', text, count=1)
    grp = {'9001': '全系统管理(9001)', '9002': '本系统管理(9002)', '9003': '公共数据(9003)'}[d['group']]
    pat = re.compile(r'(\| %s \| [^|]+?)\|' % re.escape(grp))
    def add(m):
        row = m.group(1).rstrip()
        if row.endswith('/') or row.endswith('、'):
            return row + '%s %s |' % (d['chinese'], d['table'])
        return row + ('、' if row[-1] != '|' else '') + '%s %s |' % (d['chinese'], d['table'])
    text, n = pat.subn(add, text, count=1)
    if n == 0:
        text += '\n| %s | %s %s | gen_module 生成 |\n' % (grp, d['chinese'], d['table'])
    rec = ('\n### 新增模块：%s（%s，gen_module.py 生成）\n'
           '- 表：`%s.%s`（%s）；函数：query/add/edit/merge/del/undel 共 6 个（均含 COMMENT，接口文档可见）\n'
           '- 权限：菜单 %s 挂于 %s 之下；操作 %s\n'
           '- 前端：`#/%s` 页面，继承通用列配置/表单/树选/模糊搜索/导出能力\n'
           % (d['chinese'], datetime.date.today().isoformat(), d['schema'], d['table'], d['kind'],
              d['menu_action'], d['group'], d['actions'], d['table']))
    cl = os.path.join(ROOT, 'CHANGELOG.md')
    if os.path.exists(cl):
        open(cl, 'a', encoding='utf-8').write(rec)
        print('新增模块记录 ->', cl)
    else:
        text += rec
    return text


def patch_testplan(d, text):
    text = re.sub(r'(\d+) 个函数', lambda m: str(int(m.group(1)) + 6) + ' 个函数', text, count=1)
    text = re.sub(r'(\d+)/(\d+)\)', lambda m: '%s/%s)' % (int(m.group(1)) + 6, int(m.group(2)) + 6), text, count=1)
    text += ('\n### 场景 N：新增模块-%s（gen_module 生成）\n'
             'N1 query 带 token→0；N2 add→0 后 query total+1；N3 edit→0 字段更新；N4 del→0 isused=0；N5 undel→0；'
             'N6 无 token→100005、伪 token→100012；N7 前端 #/%s 列表/表单/搜索正常。\n' % (d['chinese'], d['table']))
    return text


def bump_index():
    p = os.path.join(HTML_DIR, 'index.html')
    s = open(p, encoding='utf-8').read()
    s2 = re.sub(r'v=(\d+)', lambda m: 'v=%d' % (int(m.group(1)) + 1), s)
    open(p, 'w', encoding='utf-8').write(s2)


def alloc_error_code(d, qfunc=None):
    """为建模表分配“记录不存在/主键冲突”专属出错代码（errorcode 表）：
       - 已有该表（schema='库.表'）记录 → 直接复用（去重，重复发布幂等）；
       - 否则取 max(errorcode)+1 新增，message='<表中文名>信息出错！'。
       返回 (code, insert_sql)；insert_sql 自带 NOT EXISTS 去重保护。
       取号失败时回退通用码 100018（行为与旧版一致）。"""
    if qfunc is None:
        def qfunc(sql):
            env = dict(os.environ, PGPASSWORD=PG['password'])
            r = subprocess.run(['psql', '-h', PG['host'], '-U', PG['user'], '-d', PG['db'],
                                '-tAc', sql], env=env, capture_output=True, text=True, timeout=15)
            return (r.stdout or '').strip()
    tag = '%s.%s' % (d['schema'], d['table'])
    try:
        exist = str(qfunc("select errorcode from sysinfo.errorcode where schema='%s' and primekey='%s' limit 1"
                          % (tag, d['pk']))).strip()
        if exist.isdigit():
            return int(exist), None
        mx = str(qfunc("select coalesce(max(errorcode),100036)+1 from sysinfo.errorcode")).strip()
        code = int(mx) if mx.isdigit() else 100037
    except Exception as e:
        print('errorcode 取号失败，回退 100018:', e)
        return 100018, None
    ins = ("INSERT INTO sysinfo.errorcode(errorcode,message,primekey,schema,isused) "
           "SELECT %d,'%s信息出错！','%s','%s',1 WHERE NOT EXISTS "
           "(SELECT 1 FROM sysinfo.errorcode WHERE schema='%s' OR errorcode=%d);\n"
           % (code, d.get('chinese') or d['table'], d['pk'], tag, tag, code))
    return code, ins


def apply_error_code(sql, code):
    """把生成函数里的通用码 100018（记录不存在/主键已存在）替换为该表专属码，
       使前端报错信息与实际表对应（100014/100016/100017 为通用语义码，保持不变）。"""
    code = int(code or 100018)
    if code != 100018:
        sql = sql.replace('100018', str(code))
    return sql


def apply_sql(sql, tx):
    env = dict(os.environ, PGPASSWORD=PG['password'])
    if tx:
        sql = 'BEGIN;\n' + sql + '\nROLLBACK;'
    tmp = os.path.join(tempfile.gettempdir(), 'gen_module_apply.sql')
    open(tmp, 'w', encoding='utf-8').write(sql)
    r = subprocess.run(['psql', '-h', PG['host'], '-U', PG['user'], '-d', PG['db'],
                        '-v', 'ON_ERROR_STOP=1', '-f', tmp], env=env, capture_output=True, text=True)
    print(r.stdout[-3000:], r.stderr[-2000:])
    if r.returncode:
        sys.exit('SQL 执行失败' + ('（事务已回滚，无痕）' if tx else ''))
    print('SQL 执行成功' + ('（事务已回滚，仅验证语法）' if tx else ''))


def deploy():
    for f in ('index.html',):
        shutil.copy(os.path.join(HTML_DIR, f), os.path.join(DEPLOY_DIR, f))
    for f in ('app.js', 'modules.js', 'suite.css'):
        shutil.copy(os.path.join(HTML_DIR, 'assets', f), os.path.join(DEPLOY_DIR, 'assets', f))
    print('已部署 html 到 ' + DEPLOY_DIR + '（nginx worker 需 reload/restart 后新接口才可用）')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--def', dest='deffile', required=True)
    ap.add_argument('--apply', action='store_true', help='把生成的 SQL 执行到在用库')
    ap.add_argument('--apply-tx', action='store_true', help='事务内试跑 SQL 后回滚（仅验证）')
    ap.add_argument('--deploy', action='store_true', help='部署 html 到 /opt/nginx/html/ngxdb')
    ap.add_argument('--print-sql', action='store_true', help='打印生成的 SQL')
    a = ap.parse_args()
    d = load_def(a.deffile)
    fks = get_fks(d)
    code, ins = alloc_error_code(d)
    sql = apply_error_code(gen_sql(d, fks), code)
    if ins:
        sql += '\n-- 出错代码（建模生成，已去重）\n' + ins
    if a.print_sql:
        print(sql)
        print('（--print-sql 仅预览：不写入 SQL 事实源，也不改动 modules.js/index/README/TEST_PLAN/CHANGELOG）')
        return
    open(SQL_FILE, 'a', encoding='utf-8').write(sql)
    print('已追加 SQL ->', SQL_FILE)
    mp = os.path.join(HTML_DIR, 'assets', 'modules.js')
    open(mp, 'a', encoding='utf-8').write('\n' + gen_modules_js(d))
    print('已追加模块配置 ->', mp)
    bump_index(); print('index.html 版本号 +1')
    rp = os.path.join(ROOT, 'README.md')
    open(rp, 'w', encoding='utf-8').write(patch_readme(d, open(rp, encoding='utf-8').read()))
    tp = os.path.join(ROOT, 'test', 'TEST_PLAN.md')
    open(tp, 'w', encoding='utf-8').write(patch_testplan(d, open(tp, encoding='utf-8').read()))
    print('README.md / test/TEST_PLAN.md 已更新')
    if a.apply:
        apply_sql(sql, False)
    if a.apply_tx:
        apply_sql(sql, True)
    if a.deploy:
        deploy()
    print('完成。记得执行 /_reloadfunctions 热重载（或 reload nginx）以刷新函数缓存。')


if __name__ == '__main__':
    try:
        main()
    except ValueError as e:
        sys.exit(str(e))

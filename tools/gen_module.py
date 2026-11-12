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
PG = dict(host='127.0.0.1', user='postgres', password='gao@12345', db='postgres')
VERBS = {'add': '新增', 'edit': '修改', 'merge': '合并', 'del': '删除', 'undel': '恢复', 'query': '查询'}


def load_def(path):
    d = json.load(open(path, encoding='utf-8'))
    for k in ('kind', 'table', 'chinese', 'pk', 'menu_action', 'actions', 'fields'):
        if k not in d:
            sys.exit('定义缺少必填项: ' + k)
    if d['kind'] not in ('single', 'parent_child', 'tree'):
        sys.exit('kind 必须是 single/parent_child/tree')
    for op in ('add', 'edit', 'merge', 'del', 'undel', 'query'):
        if op not in d['actions']:
            sys.exit('actions 缺少 ' + op)
    d.setdefault('schema', 'sysinfo')
    d.setdefault('group', '9001')
    d.setdefault('icon', 'list')
    d.setdefault('systemid', 100)
    if d['kind'] == 'parent_child':
        for k in ('parent_table', 'parent_pk', 'parent_name'):
            if not d.get(k):
                sys.exit('parent_child 必须提供 ' + k)
    return d


def pg_type(t):
    t = (t or 'text').strip().lower()
    if t.startswith('varchar') or t.startswith('character varying'):
        return 'character varying'
    if t in ('int', 'integer', 'int4'):
        return 'integer'
    if t in ('bigint', 'int8'):
        return 'bigint'
    if t in ('smallint', 'int2'):
        return 'smallint'
    if t.startswith('timestamp'):
        return 'timestamp without time zone'
    if t in ('numeric', 'decimal'):
        return 'numeric'
    if t in ('bool', 'boolean'):
        return 'boolean'
    if t == 'date':
        return 'date'
    return 'text'


def is_text(f):
    return pg_type(f.get('type')) in ('character varying', 'text')


def biz_fields(d):
    """业务字段（排除 isused/upid/idpath 等结构列）"""
    return [f for f in d['fields'] if f['name'] not in ('isused', 'upid', 'idpath', 'idlevel', 'idcount', d['pk'])]


def gen_create_table(d):
    s, t, pk = d['schema'], d['table'], d['pk']
    cols = ['  %s bigserial primary key' % pk]
    if d['kind'] in ('parent_child', 'tree'):
        cols.append('  upid bigint default 0')
    if d['kind'] == 'tree':
        cols += ['  idpath character varying(255)', '  idlevel integer', '  idcount integer']
    for f in biz_fields(d):
        cols.append('  %s %s' % (f['name'], f.get('type', 'text')))
    cols.append('  isused smallint default 1')
    cols.append('  systemid integer')
    return ('-- ======== %s（%s，%s） ========\n'
            'CREATE TABLE IF NOT EXISTS %s.%s (\n%s\n);\n'
            'ALTER TABLE %s.%s OWNER TO gm;\n'
            % (t, d['chinese'], d['kind'], s, t, ',\n'.join(cols), s, t))


def where_clause(d, alias='t'):
    """query 的 where 条件：文本 position 模糊，数值/时间相等"""
    conds = []
    for f in biz_fields(d):
        n = f['name']
        if is_text(f):
            conds.append('(%s is null or position(%s in %s.%s)>0)' % ('p_' + n, 'p_' + n, alias, n))
        else:
            conds.append('(%s is null or %s.%s=%s)' % ('p_' + n, alias, n, 'p_' + n))
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


def select_list(d):
    s, t, pk = d['schema'], d['table'], d['pk']
    items = ['t.*']
    if d['kind'] == 'parent_child':
        items += ['pp.%s as upname' % (d.get('parent_name') or name_field(d))]
    if d['kind'] == 'tree':
        items += ['tt.%s as upname' % name_field(d)]
    return ', '.join(items)


def from_clause(d):
    s, t, pk = d['schema'], d['table'], d['pk']
    frm = ' from %s.%s t' % (s, t)
    if d['kind'] == 'parent_child':
        frm += ' left join %s.%s pp on t.upid=pp.%s' % (s, d['parent_table'], d['parent_pk'])
    if d['kind'] == 'tree':
        frm += ' left join %s.%s tt on t.upid=tt.%s' % (s, t, pk)
    return frm


def tree_recalc(d):
    """照抄 sysorg 的全表 idpath/idlevel/idcount 重算"""
    s, t, pk = d['schema'], d['table'], d['pk']
    return ("  with recursive tree as\n"
            "  ( select %(pk)s,upid,1 as idlevel,%(pk)s::varchar as idpath,t1.systemid from %(s)s.%(t)s t1 where upid=0\n"
            "  union\n"
            "  select t1.%(pk)s,t1.upid,t2.idlevel+1,t2.idpath||'.'||t1.%(pk)s,t1.systemid from %(s)s.%(t)s t1 inner join tree t2 on t2.%(pk)s=t1.upid and t2.systemid=t1.systemid where isused<>0)\n"
            "  update %(s)s.%(t)s t set idpath=t1.idpath,idlevel=t1.idlevel,idcount=coalesce(cc,0) from tree t1 left join (select upid,count(*) cc,systemid from %(s)s.%(t)s where isused<>0 group by upid,systemid) t2 on t1.%(pk)s=t2.upid\n"
            "  where t.%(pk)s=t1.sysorgid;" % {'s': s, 't': t, 'pk': pk}).replace('t1.sysorgid', 't1.' + pk)


def fn_head(d, op, params):
    s, t = d['schema'], d['table']
    return ('CREATE OR REPLACE FUNCTION %s.%s_%s(%s) RETURNS json\n'
            '    LANGUAGE plpgsql SECURITY DEFINER\n    AS $$\n') % (s, t, op, params)


def gen_query(d):
    s, t, pk = d['schema'], d['table'], d['pk']
    params = 'p_token character varying, p_rows integer, p_page integer'
    params += ''.join(', p_%s %s' % (f['name'], pg_type(f['type'])) for f in biz_fields(d))
    params += ', p_isused integer'
    w = where_clause(d)
    sel, frm, order = select_list(d), from_clause(d), 't.' + pk
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
    return ', p_upid bigint' if d['kind'] in ('parent_child', 'tree') else ''


def _parent_check(d):
    if d['kind'] not in ('parent_child', 'tree'):
        return ''
    s, t, pk = d['schema'], d['table'], d['pk']
    pt, ppk = (d['parent_table'], d['parent_pk']) if d['kind'] == 'parent_child' else (t, pk)
    return ("  if p_upid is not null then\n"
            "    select count(*) into v_c from {s}.{pt} where {ppk}=p_upid and isused<>0;\n"
            "    if v_c=0 then return gm.returnjson(100014);end if;--上级不存在\n"
            "  end if;\n").format(s=s, pt=pt, ppk=ppk)


def _loop_check(d, idvar='v_idpath'):
    """树形：上级不能是自身或其下级（循环定义）"""
    if d['kind'] != 'tree':
        return ''
    s, t, pk = d['schema'], d['table'], d['pk']
    return ("  select idpath into {v} from {s}.{t} where {pk}=p_upid and isused<>0;\n"
            "  if position('.'||p_{pk}||'.' in '.'||coalesce({v},'')||'.')>0 then return gm.returnjson(100017);end if;--不能循环定义\n").format(v=idvar, s=s, t=t, pk=pk)


def gen_add(d):
    s, t, pk = d['schema'], d['table'], d['pk']
    bf = biz_fields(d)
    params = 'p_token character varying' + _upid_param(d)
    if d['kind'] == 'tree':
        params += ', p_%s bigint' % pk
    params += ''.join(', p_%s %s' % (f['name'], pg_type(f['type'])) for f in bf)
    cols = ['isused', 'systemid']
    vals = ['1', "(v_check->'info'->>'systemid')::integer"]
    if d['kind'] in ('parent_child', 'tree'):
        cols.insert(0, 'upid'); vals.insert(0, 'coalesce(p_upid,0)')
    if d['kind'] == 'tree':
        cols.insert(0, pk); vals.insert(0, 'v_id')
    cols += [f['name'] for f in bf]; vals += ['p_' + f['name'] for f in bf]
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
    sql += ("declare v_c int;{idvar} int;v_check json;\nbegin\n"
            "  v_check:=gm.check_login(p_token,{a});\n"
            "  if v_check->>'errorcode'<>'0' then return v_check; end if;\n").format(idvar='v_id' if d['kind'] == 'tree' else 'v_n', a=d['actions']['add'])
    sql += _parent_check(d) + seq
    sql += ("  insert into {s}.{t}({cols}) values({vals}) returning {pk} into {idvar};\n").format(
        s=s, t=t, cols=','.join(cols), vals=','.join(vals), pk=pk, idvar='v_id' if d['kind'] == 'tree' else 'v_n')
    if d['kind'] == 'tree':
        sql += tree_recalc(d) + '\n'
    sql += "  return gm.returnjson(0,json_build_object('id',{}));\nend;\n$$;\n".format('v_id' if d['kind'] == 'tree' else 'v_n')
    return sql, params


def gen_edit(d):
    s, t, pk = d['schema'], d['table'], d['pk']
    bf = biz_fields(d)
    params = 'p_token character varying, p_%s bigint' % pk + _upid_param(d)
    params += ''.join(', p_%s %s' % (f['name'], pg_type(f['type'])) for f in bf)
    sets = ['%s=p_%s' % (f['name'], f['name']) for f in bf]
    if d['kind'] in ('parent_child', 'tree'):
        sets.insert(0, 'upid=coalesce(p_upid,upid)')
    sql = fn_head(d, 'edit', params)
    sql += ("declare v_c int;v_idpath varchar;v_check json;\nbegin\n"
            "  v_check:=gm.check_login(p_token,{a});\n"
            "  if v_check->>'errorcode'<>'0' then return v_check; end if;\n"
            "  select count(*) into v_c from {s}.{t} where {pk}=p_{pk};\n"
            "  if v_c=0 then return gm.returnjson(100018); end if;--记录不存在\n").format(a=d['actions']['edit'], s=s, t=t, pk=pk)
    sql += _parent_check(d)
    sql += _loop_check(d)
    sql += "  update {s}.{t} set {sets} where {pk}=p_{pk};\n".format(s=s, t=t, sets=','.join(sets), pk=pk)
    if d['kind'] == 'tree':
        sql += tree_recalc(d) + '\n'
    sql += '  return gm.returnjson(0);\nend;\n$$;\n'
    return sql, params


def gen_merge(d):
    s, t, pk = d['schema'], d['table'], d['pk']
    bf = biz_fields(d)
    params = 'p_token character varying' + _upid_param(d) + ', p_%s bigint' % pk
    params += ''.join(', p_%s %s' % (f['name'], pg_type(f['type'])) for f in bf)
    sets = ['%s=p_%s' % (f['name'], f['name']) for f in bf]
    if d['kind'] in ('parent_child', 'tree'):
        sets.insert(0, 'upid=coalesce(p_upid,upid)')
    sets.append('systemid=(v_check->\'info\'->>\'systemid\')::integer')
    icols = ['isused', 'systemid']
    ivals = ['1', "(v_check->'info'->>'systemid')::integer"]
    if d['kind'] in ('parent_child', 'tree'):
        icols.insert(0, 'upid'); ivals.insert(0, 'coalesce(p_upid,0)')
    if d['kind'] == 'tree':
        icols.insert(0, pk); ivals.insert(0, 'v_id')
    icols += [f['name'] for f in bf]; ivals += ['p_' + f['name'] for f in bf]
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
    sql += _parent_check(d)
    sql += _loop_check(d) + seq
    sql += ("  with \"te\" as (update {s}.{t} set {sets} where {pk}=p_{pk} returning *)\n"
            "  insert into {s}.{t} ({cols}) select {vals} where (select count(*) from te)=0;\n").format(
        s=s, t=t, sets=','.join(sets), pk=pk, cols=','.join(icols), vals=','.join(ivals))
    if d['kind'] == 'tree':
        sql += tree_recalc(d) + '\n'
    else:
        sql += ("  if v_id is null then\n"
                "    select currval(pg_get_serial_sequence('{s}.{t}','{pk}')) into v_id;\n"
                "  end if;\n").format(s=s, t=t, pk=pk)
    sql += "  return gm.returnjson(0,json_build_object('id',v_id));\nend;\n$$;\n"
    return sql, params


def gen_del(d, op):
    s, t, pk = d['schema'], d['table'], d['pk']
    flag = '0' if op == 'del' else '1'
    sql = fn_head(d, op, 'p_token character varying, p_%s bigint' % pk)
    sql += ("declare v_c int;v_check json;\nbegin\n"
            "  v_check:=gm.check_login(p_token,{a});\n"
            "  if v_check->>'errorcode'<>'0' then return v_check; end if;\n"
            "  select count(*) into v_c from {s}.{t} where {pk}=p_{pk};\n"
            "  if v_c=0 then return gm.returnjson(100018); end if;--记录不存在\n").format(a=d['actions'][op], s=s, t=t, pk=pk)
    if op == 'del' and d['kind'] == 'tree':
        sql += ("  select count(*) into v_c from {s}.{t} where upid=p_{pk} and isused<>0;\n"
                "  if v_c>0 then return gm.returnjson(100016);end if;--有下级不能删除\n").format(s=s, t=t, pk=pk)
    sql += "  update {s}.{t} set isused={f} where {pk}=p_{pk};\n".format(s=s, t=t, f=flag, pk=pk)
    if d['kind'] == 'tree':
        sql += tree_recalc(d) + '\n'
    sql += '  return gm.returnjson(0);\nend;\n$$;\n'
    return sql, 'p_token character varying, p_%s bigint' % pk


def gen_comment(d, op, params):
    s, t, pk = d['schema'], d['table'], d['pk']
    lines = ['%s%s' % (VERBS[op], d['chinese'])]
    if op in ('add', 'edit', 'merge'):
        if d['kind'] in ('parent_child', 'tree'):
            lines.append('upid:上级id（%s）' % (d.get('parent_table') or t))
        for f in biz_fields(d):
            lines.append('%s:%s' % (f['name'], f.get('cn', '')))
    elif op == 'query':
        lines.append('参数：token/rows/page + 各业务字段（文本模糊、数值精确）+ isused；rows 为空返回全量')
    else:
        lines.append('%s:%s' % (pk, '主键'))
    tail = '{"errorcode":0,"message":"执行成功！"}'
    if op in ('add', 'merge'):
        tail = '{"errorcode":0,"message":"执行成功！","info":{"id":id}}'
    elif op == 'query':
        tail = '{"errorcode":0,"message":"执行成功！","info":{"total":n,"rows":[...]}}'
    lines.append('返回：' + tail)
    body = '\n'.join(lines).replace("'", "''")
    return 'COMMENT ON FUNCTION %s.%s_%s(%s) IS \'%s\';\n\n' % (s, t, op, params, body)


def gen_sysaction(d):
    g, m = d['group'], d['menu_action']
    rows = [(1, '100.%s.%s' % (g, m), 3, 6, int(g), None, d['systemid'], m, d['chinese'], None, None)]
    for op, act in d['actions'].items():
        rows.append((1, '100.%s.%s.%s' % (g, m, act), 4, 0, m, None, d['systemid'], act, VERBS[op] + d['chinese'], act, None))
    sql = '-- 权限节点（菜单 %s：%s，挂于 %s 之下）\n' % (m, d['chinese'], g)
    for r in rows:
        vals = ', '.join('NULL' if v is None else (str(v) if not isinstance(v, str) else "'%s'" % v) for v in r)
        sql += ('INSERT INTO sysinfo.sysaction (isused,idpath,idlevel,idcount,upid,params,systemid,sysactionid,sysactionname,actionid,isdefault)\n'
                'VALUES (%s) ON CONFLICT (sysactionid) DO NOTHING;\n' % vals)
    return sql + '\n'


def gen_sql(d):
    s, t = d['schema'], d['table']
    out = ['\n-- ==================== [%s] %s.%s：由 tools/gen_module.py 生成 %s ====================\n'
           % (d['kind'], s, t, datetime.date.today().isoformat())]
    out.append(gen_create_table(d))
    gens = {'query': gen_query, 'add': gen_add, 'edit': gen_edit, 'merge': gen_merge}
    for op in ('query', 'add', 'edit', 'merge', 'del', 'undel'):
        if op in gens:
            sql, params = gens[op](d)
        else:
            sql, params = gen_del(d, op)
        out.append('--\n-- Name: %s_%s(%s); Type: FUNCTION; Schema: %s; Owner: gm\n--\n\n' % (t, op, params, s))
        out.append(sql)
        out.append('ALTER FUNCTION %s.%s_%s(%s) OWNER TO gm;\n\n' % (s, t, op, params))
        out.append(gen_comment(d, op, params))
    out.append(gen_sysaction(d))
    return '\n'.join(out)


def gen_modules_js(d):
    s, t, pk = d['schema'], d['table'], d['pk']
    cols, fields = ['{f:%r}' % pk], []
    if d['kind'] in ('parent_child', 'tree'):
        cols.append('{f:%r}' % 'upid'); cols.append('{f:%r}' % 'upname')
    for f in biz_fields(d):
        c = '{f:%r}' % f['name']
        if f.get('wide'): c = c[:-1] + ',w:1}'
        if f.get('flag'): c = c[:-1] + ',b:1}'
        cols.append(c)
        fd = '{f:%r' % f['name']
        if f['name'] == pk or f.get('required'): fd += ',r:1'
        if f.get('flag'): fd += ',sw:1'
        fd += '}'
        fields.append(fd)
    if not any(f['name'] == 'isused' for f in d['fields']):
        cols.append('{f:%r,b:1}' % 'isused')
    tree = '1' if d['kind'] == 'tree' else 'null'
    tp = "tp:'%s'" % (d['parent_table'] if d['kind'] == 'parent_child' else t)
    if d['kind'] in ('parent_child', 'tree'):
        fields.insert(1, '{f:upid,%s}' % tp)
    line = ("/* %s（%s）：由 tools/gen_module.py 生成 */\n"
            "P('%s','%s','%s',%s,'%s','%s/%s','%s',\n"
            "  [%s],\n  [%s],\n  {add:%s,edit:%s,merge:%s,del:%s,undel:%s},null,%s);\n"
            % (t, d['chinese'], t, d['chinese'], d['group'], d['menu_action'], d['icon'], s, t, pk,
               ','.join(cols), ','.join(fields), d['actions']['add'], d['actions']['edit'],
               d['actions']['merge'], d['actions']['del'], d['actions']['undel'], tree))
    return line


def patch_readme(d, text):
    text = re.sub(r'(\d+) 函数', lambda m: str(int(m.group(1)) + 6) + ' 函数', text, count=1)
    grp = {'9001': '全系统管理(9001)', '9002': '本系统管理(9002)', '9003': '公共数据(9003)'}[d['group']]
    pat = re.compile(r'(\| %s \| [^|]+)\|' % re.escape(grp))
    def add(m):
        row = m.group(1).rstrip()
        return row + (' / ' if not row.endswith('/') else '') + '%s %s |' % (d['chinese'], d['table'])
    text, n = pat.subn(add, text, count=1)
    if n == 0:
        text += '\n| %s | %s %s | gen_module 生成 |\n' % (grp, d['chinese'], d['table'])
    text += ('\n### 新增模块：%s（%s，gen_module.py 生成）\n'
             '- 表：`%s.%s`（%s）；函数：query/add/edit/merge/del/undel 共 6 个（均含 COMMENT，接口文档可见）\n'
             '- 权限：菜单 %s 挂于 %s 之下；操作 %s\n'
             '- 前端：`#/%s` 页面，继承通用列配置/表单/树选/模糊搜索/导出能力\n'
             % (d['chinese'], datetime.date.today().isoformat(), d['schema'], d['table'], d['kind'],
                d['menu_action'], d['group'], d['actions'], d['table']))
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
    sql = gen_sql(d)
    if a.print_sql:
        print(sql)
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
    print('完成。记得重启/重载 nginx 以刷新函数缓存。')


if __name__ == '__main__':
    main()

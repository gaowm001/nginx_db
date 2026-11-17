#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""gm Python 模式：为单表/父子表/树表生成 plpython3u 版 CRUD 六函数。

签名与 tools/gen_module.py 的 plpgsql 版完全一致（参数名/顺序相同），
因此 modules.js 前端配置、COMMENT、sysaction 权限节点可完全复用。
支持 kind='single'（单表）、'parent_child'（父子表）、'tree'（树表）。

与 plpgsql 版的语义对齐点：
  - tree：p_upid 上级存在性(100014)、循环定义(100017)、自定义主键取号(100018)、
    idpath/idlevel/idcount 全表重算、有下级禁删(100016)、query 返回 upname 按 idpath 排序；
  - parent_child：p_<child>json 整表 JSON 随父表 add/edit/merge 一次性保存
    （按子表主键增/删/改，不先删后插，保持子表主键稳定）、query 每行返回 <child>json、
    有子记录禁删(100016)。
"""
import os
import re
import sys

_TOOLS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'tools')
if _TOOLS not in sys.path:
    sys.path.insert(0, _TOOLS)
import gen_module as gm  # noqa: E402


def _pytype(t):
    """pg 类型 -> plpy.prepare 参数类型名"""
    t = gm.pg_type(t)
    return {'character varying': 'varchar',
            'timestamp without time zone': 'timestamp'}.get(t, t)


def _head(d, op, params):
    return ('CREATE OR REPLACE FUNCTION %s.%s_%s(%s) RETURNS json\n'
            '    LANGUAGE plpython3u SECURITY DEFINER\n'
            '    AS $$\n' % (d['schema'], d['table'], op, params))


def _check(d, act):
    """登录校验 + 返回结构准备（Python 前置块）"""
    return ('import json\n'
            '_chk = plpy.execute(plpy.prepare('
            '"select gm.check_login($1,$2)::text as r", ["text", "integer"]), '
            '[p_token, ' + str(act) + '])\n'
            '_v_check = json.loads(_chk[0]["r"])\n'
            'if _v_check.get("errorcode") != 0:\n'
            '    return json.dumps(_v_check)\n'
            '_sysid = (_v_check.get("info") or {}).get("systemid")\n')


def _rj(code, payload=None):
    """经 gm.returnjson 返回，保证与 plpgsql 版输出结构一致"""
    pay = 'json.dumps(%s)' % payload if payload is not None else 'None'
    return ('_rj = plpy.execute(plpy.prepare('
            '"select gm.returnjson($1,$2)::text as r", ["int", "json"]), '
            '[' + str(code) + ', ' + pay + '])\n'
            'return _rj[0]["r"]\n')


def _params_str(items):
    return ', '.join('%s %s' % (n, t) for n, t in items)


def _ind(code, n=1):
    """将代码块整体缩进 n 级（每级 4 空格），用于嵌套 if 内的 return"""
    pad = '    ' * n
    return ('\n' + pad).join(code.strip('\n').split('\n'))


def _fail(code, pad=''):
    """嵌套块内的 return gm.returnjson(...)；pad 为每行统一前缀（含首行）"""
    return pad + ('\n' + pad).join(_rj(code).strip('\n').split('\n')) + '\n'


def _childjson_items(d):
    """父子表 add/edit/merge 追加的子表 JSON 入参"""
    cc = gm.child_cfg(d)
    return [('p_%sjson' % cc[0], 'json')] if cc else []


def _child_sync_py(d, idexpr):
    """父子表：子表整表 JSON 按主键增/删/改（对齐 plpgsql 版 _child_sync）。
    不做先删后插，保持子表主键稳定；idexpr 为父主键的 Python 表达式文本。"""
    cc = gm.child_cfg(d)
    if not cc:
        return ''
    s, ct, cpk, cfk = d['schema'], cc[0], cc[1], cc[2]
    bf = gm.biz_of(cc[3], cpk)
    defs = ', '.join(['%s bigint' % cpk] + ['%s %s' % (f['name'], gm._coltype(f.get('type'))) for f in bf])
    rs = "json_to_recordset(coalesce($1,'[]'::json)) as x(%s)" % defs
    cols = ', '.join(f['name'] for f in bf)
    sets = ', '.join('%s=x.%s' % (f['name'], f['name']) for f in bf)
    sell = ', '.join('x.' + f['name'] for f in bf)
    code = ('# 子表按主键增删改：1) 删除该父行下、主键不在提交集合中的行\n'
            '#   （delete 必须在 insert 之前，避免误删本次新插入、主键尚不在提交集合中的行）\n')
    sql1 = ('delete from {s}.{ct} t3 where t3.{cfk} = $2 and not exists '
            '(select 1 from {rs} where x.{cpk} = t3.{cpk})').format(
        s=s, ct=ct, cfk=cfk, rs=rs, cpk=cpk)
    code += 'plpy.execute(plpy.prepare(%r, ["json", "bigint"]), [p_%sjson, %s])\n' % (sql1, ct, idexpr)
    sql2 = ('update {s}.{ct} t set {sets}, isused = 1, systemid = $3 from {rs} '
            'where t.{cpk} = x.{cpk} and t.{cfk} = $2 and x.{cpk} is not null').format(
        s=s, ct=ct, sets=sets, rs=rs, cpk=cpk, cfk=cfk)
    code += 'plpy.execute(plpy.prepare(%r, ["json", "bigint", "integer"]), [p_%sjson, %s, _sysid])\n' % (sql2, ct, idexpr)
    sql3 = ('insert into {s}.{ct}({cfk},{cpk},isused,systemid,{cols}) '
            "select $2, nextval('{s}.{ct}_{cpk}_seq'), 1, $3, {sell} from {rs} "
            'where x.{cpk} is null or not exists '
            '(select 1 from {s}.{ct} t2 where t2.{cpk} = x.{cpk})').format(
        s=s, ct=ct, cfk=cfk, cpk=cpk, cols=cols, sell=sell, rs=rs)
    code += 'plpy.execute(plpy.prepare(%r, ["json", "bigint", "integer"]), [p_%sjson, %s, _sysid])\n' % (sql3, ct, idexpr)
    return code


def _parent_check_py(d):
    """树表：上级必须存在且未删除（对齐 plpgsql 错误码 100014）"""
    s, t, pk = d['schema'], d['table'], d['pk']
    return ('if p_upid is not None:\n'
            '    _pc = plpy.execute(plpy.prepare("select count(*)::int as c from '
            + s + '.' + t + ' where ' + pk + ' = $1 and isused <> 0", ["bigint"]), [p_upid])[0]["c"]\n'
            '    if _pc == 0:\n'
            + _fail(100014, '        '))


def _loop_check_py(d):
    """树表：上级不能是自身或其下级（循环定义，100017）"""
    s, t, pk = d['schema'], d['table'], d['pk']
    return ('if p_upid is not None and p_' + pk + ' is not None and p_upid == p_' + pk + ':\n'
            + _fail(100017, '    ')
            + 'if p_upid is not None:\n'
            '    _ip = plpy.execute(plpy.prepare("select idpath from ' + s + '.' + t
            + ' where ' + pk + ' = $1 and isused <> 0", ["bigint"]), [p_upid])\n'
            '    _idpath = _ip[0]["idpath"] if _ip else None\n'
            '    if p_' + pk + ' is not None and _idpath and (".%s." % p_' + pk + ') in (".%s." % _idpath):\n'
            + _fail(100017, '        '))


def _pkgen_py(d):
    """树表 add：主键可传入（查重 100018）或由序列循环取号"""
    s, t, pk = d['schema'], d['table'], d['pk']
    nxt = "select nextval('%s')::text as n" % (s + '.' + t + '_' + pk + '_seq')
    return ('_ex = plpy.prepare("select count(*)::int as c from ' + s + '.' + t
            + ' where ' + pk + ' = $1", ["bigint"])\n'
            'if p_' + pk + ' is None:\n'
            '    v_id = None\n'
            '    while True:\n'
            '        v_id = int(plpy.execute(plpy.prepare(' + repr(nxt) + ', []), [])[0]["n"])\n'
            '        if plpy.execute(_ex, [v_id])[0]["c"] == 0:\n'
            '            break\n'
            'else:\n'
            '    if plpy.execute(_ex, [p_' + pk + '])[0]["c"] > 0:\n'
            + _fail(100018, '        ')
            + '    v_id = int(p_' + pk + ')\n')


def _recalc_py(d):
    """树表：全表 idpath/idlevel/idcount 重算（复用 plpgsql tree_recalc 语句）"""
    return 'plpy.execute(%r)\n' % gm.tree_recalc(d).strip()


def gen_query_py(d):
    s, t, pk = d['schema'], d['table'], d['pk']
    bf = gm.biz_fields(d)
    cc = gm.child_cfg(d)
    tree = d['kind'] == 'tree'
    items = [('p_token', 'character varying'), ('p_rows', 'integer'), ('p_page', 'integer')]
    items += [('p_' + f['name'], gm.pg_type(f.get('type', 'text'))) for f in bf]
    items += [('p_isused', 'integer')]
    params = _params_str(items)
    code = _head(d, 'query', params)
    code += _check(d, d['actions']['query'])
    # select 列表：父子表附加 <child>json、树表附加 upname（对齐 plpgsql select_list）
    sel = 't.*'
    if cc:
        sel += (', (select array_to_json(array_agg(row_to_json(c))) from '
                '(select c1.* from {s}.{ct} c1 where c1.{cfk} = t.{pk} order by c1.{cpk}) c) as {ct}json'
                ).format(s=s, ct=cc[0], cfk=cc[2], pk=pk, cpk=cc[1])
    if tree:
        sel += ', tt.%s as upname' % gm.name_field(d)
    frm = ' from ' + s + '.' + t + ' t'
    if tree:
        frm += ' left join ' + s + '.' + t + ' tt on t.upid = tt.' + pk
    order = 't.idpath' if tree else 't.' + pk
    # where：文本模糊/数值相等，NULL 参数不过滤（对齐 plpgsql where_clause）
    conds, cparams, ctypes = [], [], []
    for f in bf:
        i = len(ctypes) + 1
        if gm.is_text(f):
            conds.append('($%d is null or position($%d in t.%s) > 0)' % (i, i, f['name']))
        else:
            conds.append('($%d is null or t.%s = $%d)' % (i, f['name'], i))
        cparams.append('p_' + f['name'])
        ctypes.append(_pytype(f.get('type', 'text')))
    if d.get('has_isused', True):
        i = len(ctypes) + 1
        conds.append('($%d is null or t.isused = $%d)' % (i, i))
        cparams.append('p_isused')
        ctypes.append('integer')
    code += ('conds = ' + repr(conds) + '\n'
             'cparams = [' + ', '.join(cparams) + ']\n'
             'ctypes = ' + repr(ctypes) + '\n'
             'where = " where " + " and ".join(conds)\n'
             'base = "select ' + sel + ' ' + frm + '" + where + " order by ' + order + '"\n'
             'cnt = "select count(*)::int as c ' + frm + '" + where\n'
             'if p_rows is None:\n'
             '    plan = plpy.prepare("select array_to_json(array_agg(row_to_json(t)))::text as j from (" + base + ") t", ctypes)\n'
             '    j = plpy.execute(plan, cparams)[0]["j"]\n'
             '    payload = {"total": (len(json.loads(j)) if j else 0), "rows": (json.loads(j) if j else [])}\n'
             'else:\n'
             '    total = plpy.execute(plpy.prepare(cnt, ctypes), cparams)[0]["c"]\n'
             '    lim = max(int(p_rows), 1)\n'
             '    cap = (total // lim - 1 + abs(total % lim)) * lim\n'
             '    off = max(cap, 0) if p_page is None else max(min((int(p_page) - 1) * lim, cap), 0)\n'
             '    plan = plpy.prepare("select array_to_json(array_agg(row_to_json(t)))::text as j from (" + base + " limit " + str(lim) + " offset " + str(off) + ") t", ctypes)\n'
             '    j = plpy.execute(plan, cparams)[0]["j"]\n'
             '    payload = {"total": total, "rows": (json.loads(j) if j else [])}\n')
    code += _rj(0, 'payload')
    code += '$$;\n'
    return code, params


def _exists_check(d, act):
    """edit/merge 前置：登录校验 + 主键存在性检查（对齐 plpgsql 版错误码 100018）"""
    s, t, pk = d['schema'], d['table'], d['pk']
    return _check(d, act) + (
        '_cnt = plpy.execute(plpy.prepare("select count(*)::int as c from '
        + s + '.' + t + ' where ' + pk + ' = $1", ["bigint"]), [p_' + pk + '])[0]["c"]\n'
        'if _cnt == 0:\n'
        '    ' + _rj(100018).strip().replace('\n', '\n    ') + '\n')


def gen_add_py(d):
    s, t, pk = d['schema'], d['table'], d['pk']
    bf = gm.biz_fields(d)
    cc = gm.child_cfg(d)
    tree = d['kind'] == 'tree'
    items = [('p_token', 'character varying')]
    if tree:
        items += [('p_upid', 'bigint'), ('p_' + pk, 'bigint')]
    items += [('p_' + f['name'], gm.pg_type(f.get('type', 'text'))) for f in bf]
    items += _childjson_items(d)
    params = _params_str(items)
    code = _head(d, 'add', params)
    code += _check(d, d['actions']['add'])
    if tree:
        code += _parent_check_py(d) + _loop_check_py(d) + _pkgen_py(d)
    cols, vals, ctypes, cparams = [], [], [], []
    if tree:
        cols += [pk, 'upid']
        vals += ['$%d' % (len(ctypes) + 1), 'coalesce($%d,0)' % (len(ctypes) + 2)]
        ctypes += ['bigint', 'bigint']
        cparams += ['v_id', 'p_upid']
    if d.get('has_isused', True):
        cols.append('isused')
        vals.append('1')
    if d.get('has_sysid', True):
        cols.append('systemid')
        vals.append('$%d' % (len(ctypes) + 1))
        ctypes.append('integer')
        cparams.append('_sysid')
    for f in bf:
        cols.append(f['name'])
        vals.append('$%d' % (len(ctypes) + 1))
        ctypes.append(_pytype(f.get('type', 'text')))
        cparams.append('p_' + f['name'])
    code += ('plan = plpy.prepare("insert into ' + s + '.' + t + ' ('
             + ', '.join(cols) + ') values (' + ', '.join(vals)
             + ') returning ' + pk + '::text as id", ' + repr(ctypes) + ')\n'
             'newid = plpy.execute(plan, [' + ', '.join(cparams) + '])[0]["id"]\n')
    if tree:
        code += _recalc_py(d)
    if cc:
        code += _child_sync_py(d, 'int(newid)')
    code += _rj(0, '{"id": int(newid)}')
    code += '$$;\n'
    return code, params


def gen_edit_py(d):
    s, t, pk = d['schema'], d['table'], d['pk']
    bf = gm.biz_fields(d)
    cc = gm.child_cfg(d)
    tree = d['kind'] == 'tree'
    items = [('p_token', 'character varying'), ('p_' + pk, 'bigint')]
    if tree:
        items += [('p_upid', 'bigint')]
    items += [('p_' + f['name'], gm.pg_type(f.get('type', 'text'))) for f in bf]
    items += _childjson_items(d)
    params = _params_str(items)
    code = _head(d, 'edit', params)
    code += _exists_check(d, d['actions']['edit'])
    if tree:
        code += _parent_check_py(d) + _loop_check_py(d)
    sets, ctypes, cparams = [], [], []
    if tree:
        sets.append('upid = coalesce($%d, upid)' % (len(ctypes) + 1))
        ctypes.append('bigint')
        cparams.append('p_upid')
    for f in bf:
        sets.append('%s = $%d' % (f['name'], len(ctypes) + 1))
        ctypes.append(_pytype(f.get('type', 'text')))
        cparams.append('p_' + f['name'])
    ctypes.append('bigint')
    cparams.append('p_' + pk)
    code += ('plpy.execute(plpy.prepare("update ' + s + '.' + t + ' set ' + ', '.join(sets)
             + ' where ' + pk + ' = $' + str(len(ctypes)) + '", '
             + repr(ctypes) + '), [' + ', '.join(cparams) + '])\n')
    if tree:
        code += _recalc_py(d)
    if cc:
        code += _child_sync_py(d, 'int(p_%s)' % pk)
    code += _rj(0)
    code += '$$;\n'
    return code, params


def gen_merge_py(d):
    s, t, pk = d['schema'], d['table'], d['pk']
    bf = gm.biz_fields(d)
    cc = gm.child_cfg(d)
    tree = d['kind'] == 'tree'
    items = [('p_token', 'character varying')]
    if tree:
        items += [('p_upid', 'bigint')]
    items += [('p_' + pk, 'bigint')]
    items += [('p_' + f['name'], gm.pg_type(f.get('type', 'text'))) for f in bf]
    items += _childjson_items(d)
    params = _params_str(items)
    code = _head(d, 'merge', params)
    code += _check(d, d['actions']['merge'])
    if tree:
        code += _parent_check_py(d) + _loop_check_py(d)
        # 主键：传参或序列循环取号（merge 不查重，对齐 plpgsql）
        nxt = "select nextval('%s')::text as n" % (s + '.' + t + '_' + pk + '_seq')
        code += ('if p_' + pk + ' is None:\n'
                 '    v_id = None\n'
                 '    while True:\n'
                 '        v_id = int(plpy.execute(plpy.prepare(' + repr(nxt) + ', []), [])[0]["n"])\n'
                 '        if plpy.execute(plpy.prepare("select count(*)::int as c from '
                 + s + '.' + t + ' where ' + pk + ' = $1", ["bigint"]), [v_id])[0]["c"] == 0:\n'
                 '            break\n'
                 'else:\n'
                 '    v_id = int(p_' + pk + ')\n')
        # upsert：update 命中则完成，否则按 v_id 插入（对齐 plpgsql with "te" ...）
        sets, ctypes, cparams = [], [], []
        sets.append('upid = coalesce($1, upid)')
        ctypes.append('bigint')
        cparams.append('p_upid')
        for f in bf:
            sets.append('%s = $%d' % (f['name'], len(ctypes) + 1))
            ctypes.append(_pytype(f.get('type', 'text')))
            cparams.append('p_' + f['name'])
        sys_i = len(ctypes) + 1
        ctypes.append('integer')
        cparams.append('_sysid')
        pk_i = len(ctypes) + 1
        ctypes.append('bigint')
        cparams.append('p_' + pk)
        vid_i = len(ctypes) + 1
        ctypes.append('bigint')
        cparams.append('v_id')
        icols = [pk, 'upid'] + (['isused'] if d.get('has_isused', True) else []) \
            + (['systemid'] if d.get('has_sysid', True) else []) + [f['name'] for f in bf]
        ivals = (['$%d' % vid_i, 'coalesce($1,0)']
                 + (['1'] if d.get('has_isused', True) else [])
                 + (['$%d' % sys_i] if d.get('has_sysid', True) else [])
                 + ['$%d' % (i + 2) for i in range(len(bf))])
        code += ('plpy.execute(plpy.prepare("with \\"te\\" as (update ' + s + '.' + t + ' set '
                 + ', '.join(sets) + ' where ' + pk + ' = $' + str(pk_i) + ' returning *) '
                 'insert into ' + s + '.' + t + ' (' + ', '.join(icols) + ') select ' + ', '.join(ivals)
                 + ' where (select count(*) from te) = 0", ' + repr(ctypes)
                 + '), [' + ', '.join(cparams) + '])\n')
        code += _recalc_py(d)
    else:
        n = len(bf)
        sets = ', '.join('%s=$%d' % (f['name'], i + 1) for i, f in enumerate(bf))
        ctypes = [_pytype(f.get('type', 'text')) for f in bf]
        cparams = ['p_' + f['name'] for f in bf]
        if d.get('has_sysid', True):
            sets += ', systemid=$%d' % (len(ctypes) + 1)
            ctypes.append('integer')
            cparams.append('_sysid')
        ctypes.append('bigint')
        cparams.append('p_' + pk)
        code += ('rows = plpy.execute(plpy.prepare("update ' + s + '.' + t + ' set ' + sets
                 + ' where ' + pk + ' = $' + str(len(ctypes)) + ' returning ' + pk + '::text as id", '
                 + repr(ctypes) + '), [' + ', '.join(cparams) + '])\n'
                 'if rows:\n'
                 '    v_id = rows[0]["id"]\n'
                 'else:\n'
                 '    cols = ' + repr((['isused'] if d.get('has_isused', True) else []) +
                                     ['systemid'] + [f['name'] for f in bf]) + '\n'
                 '    vals = ' + repr((['1'] if d.get('has_isused', True) else []) +
                                     ['$1'] + ['$' + str(i + 2) for i in range(n)]) + '\n'
                 '    iplan = plpy.prepare("insert into ' + s + '.' + t + ' (" + ", ".join(cols) + ") values (" + ", ".join(vals) + ") returning ' + pk + '::text as id", ' + repr(['integer'] + [_pytype(f.get('type', 'text')) for f in bf]) + ')\n'
                 '    v_id = plpy.execute(iplan, [_sysid] + [' + ', '.join('p_' + f['name'] for f in bf) + '])[0]["id"]\n')
    if cc:
        code += _child_sync_py(d, 'int(v_id)')
    code += _rj(0, '{"id": int(v_id)}')
    code += '$$;\n'
    return code, params


def gen_del_py(d, op):
    s, t, pk = d['schema'], d['table'], d['pk']
    cc = gm.child_cfg(d)
    tree = d['kind'] == 'tree'
    has_isused = d.get('has_isused', True)
    if not has_isused and op == 'undel':
        return None, None                        # 无 isused：无恢复语义，不生成 undel
    params = 'p_token character varying, p_%s bigint' % pk
    code = _head(d, op, params)
    code += _check(d, d['actions'][op])
    code += ('_cnt = plpy.execute(plpy.prepare("select count(*)::int as c from '
             + s + '.' + t + ' where ' + pk + ' = $1", ["bigint"]), [p_' + pk + '])[0]["c"]\n'
             'if _cnt == 0:\n'
             + _fail(100018, '    '))
    if op == 'del' and tree:
        # 有下级不能删除（100016）
        code += ('_cc = plpy.execute(plpy.prepare("select count(*)::int as c from '
                 + s + '.' + t + ' where upid = $1 and isused <> 0", ["bigint"]), [p_' + pk + '])[0]["c"]\n'
                 'if _cc > 0:\n'
                 + _fail(100016, '    '))
    if op == 'del' and cc:
        # 有子记录不能删除（100016）
        code += ('_cc = plpy.execute(plpy.prepare("select count(*)::int as c from '
                 + s + '.' + cc[0] + ' where ' + cc[2] + ' = $1 and isused <> 0", ["bigint"]), [p_' + pk + '])[0]["c"]\n'
                 'if _cc > 0:\n'
                 + _fail(100016, '    '))
    if has_isused:
        code += ('plpy.execute(plpy.prepare("update ' + s + '.' + t + ' set isused='
                 + ('0' if op == 'del' else '1') + ' where ' + pk + ' = $1", ["bigint"]), [p_' + pk + '])\n')
    else:
        # 无 isused：物理删除
        code += ('plpy.execute(plpy.prepare("delete from ' + s + '.' + t
                 + ' where ' + pk + ' = $1", ["bigint"]), [p_' + pk + '])\n')
    if tree:
        code += _recalc_py(d)
    code += _rj(0)
    code += '$$;\n'
    return code, params


def gen_all(d):
    """生成完整 SQL：建表 + plpython3u 六函数 + 注释 + 权限节点（单表/父子表/树表）"""
    if d.get('kind') not in ('single', 'parent_child', 'tree'):
        raise ValueError('gm Python 模式支持 single/parent_child/tree')
    s, t = d['schema'], d['table']
    out = ['\n-- ==================== [%s:plpython3u] %s.%s：由 admin/gen_pyfuncs.py 生成 ====================\n' % (d['kind'], s, t)]
    out.append(gm.gen_create_table(d))
    gens = {'query': gen_query_py, 'add': gen_add_py, 'edit': gen_edit_py, 'merge': gen_merge_py}
    for op in ('query', 'add', 'edit', 'merge', 'del', 'undel'):
        if op in gens:
            sql, params = gens[op](d)
        else:
            sql, params = gen_del_py(d, op)
        if sql is None:
            continue                                  # 无 isused 的表不生成 undel
        out.append('--\n-- Name: %s_%s(%s); Type: FUNCTION; Schema: %s; Owner: gm\n--\n\n' % (t, op, params, s))
        out.append(sql)
        out.append('ALTER FUNCTION %s.%s_%s(%s) OWNER TO gm;\n\n' % (s, t, op, params))
        out.append(gm.gen_comment(d, op, params))
    out.append(gm.gen_sysaction(d))
    return '\n'.join(out)


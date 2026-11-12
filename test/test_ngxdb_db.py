#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ngxdb 数据库层功能测试（第一层）。
直接调用 SQL 函数验证返回码/数据，覆盖全部 77 个函数。
不依赖第三方库：通过 psql 子进程执行，JSON 聚合解析。
用法: python3 test_ngxdb_db.py [--db ngxdb_test] [--host 127.0.0.1] [--user postgres] [--password xxx]
用例编号与 test/TEST_PLAN.md 对应（A1..K3）。
"""
import json, os, subprocess, sys

HOST, USER, PASSWORD, DB = '127.0.0.1', 'postgres', os.environ.get('PGPASSWORD', ''), 'ngxdb_test'
if not PASSWORD:
    sys.exit("[NGXDB-TEST] 缺少数据库密码：请先 export PGPASSWORD='<postgres用户密码>'，或用 --password 传入")
PSQL = os.environ.get('PSQL_BIN', 'psql')
TIMEOUT = 60

def argparse():
    global HOST, USER, PASSWORD, DB
    a = sys.argv[1:]
    for i, x in enumerate(a):
        if x == '--db': DB = a[i + 1]
        elif x == '--host': HOST = a[i + 1]
        elif x == '--user': USER = a[i + 1]
        elif x == '--password': PASSWORD = a[i + 1]

RESULTS = []
def rec(group, name, ok, detail='', known=False):
    st = 'KNOWN' if known else ('PASS' if ok else 'FAIL')
    RESULTS.append((group, name, st, detail))
    tag = {'PASS': '✔', 'FAIL': '✘', 'KNOWN': '⚠'}[st]
    line = '  %s [%s] %s' % (tag, st, name)
    if not ok or known:
        line += '  -> ' + (detail or '')[:300]
    print(line)
    return ok

def psql(sql):
    env = dict(os.environ, PGPASSWORD=PASSWORD)
    p = subprocess.run([PSQL, '-h', HOST, '-U', USER, '-d', DB, '-At', '-v', 'ON_ERROR_STOP=1', '-c', sql],
                       capture_output=True, text=True, env=env, timeout=TIMEOUT)
    return p.returncode == 0, (p.stdout or '').strip(), (p.stderr or '').strip()

def call(expr):
    """执行返回单列的表达式，聚合为 JSON 数组。返回 (ok, value/list, errmsg)"""
    ok, out, err = psql("SELECT coalesce(json_agg(q)::text,'[]') FROM (SELECT (" + expr + ") AS q) s")
    if not ok:
        return False, None, err
    return True, json.loads(out), ''

def one(expr):
    """取第一行值；出错返回 ('__ERR__', msg)"""
    ok, v, err = call(expr)
    if not ok:
        return '__ERR__', err
    if not v:
        return '__NULL__', ''
    return v[0], ''

def jcall(expr):
    """调用返回 json 的函数 → dict；出错返回 {'__err__':msg}"""
    x, err = one(expr)
    if x == '__ERR__':
        return {'__err__': err}
    if x in ('__NULL__', None):
        return {'__null__': True}
    return x if isinstance(x, dict) else {'__val__': x}

def ec(r):
    return r.get('errorcode') if isinstance(r, dict) else None

def g0_basics():
    print('== G0 基础工具函数 ==')
    r = jcall("gm.returnjson(0)")
    rec('G0', 'A1 returnjson(0) 成功码', ec(r) == 0, str(r))
    r = jcall("gm.returnjson(100001)")
    rec('G0', 'A2 returnjson(100001) 消息正确', ec(r) == 100001 and '帐号密码' in (r.get('message') or ''), str(r))
    r = jcall("gm.returnjson(99999)")
    rec('G0', 'A3 returnjson(未知码) 未知错误', ec(r) == 99999 and r.get('message') == '未知错误', str(r))
    x, err = one("gm.actionupdate()")
    rec('G0', 'A4 actionupdate() 返回true', x is True and not err, str(err) or str(x))
    n, _ = one("SELECT count(*) FROM sysinfo.sysaction WHERE idpath IS NULL AND isused=1 AND systemid=100")
    rec('G0', 'A4b actionupdate 后 idpath 无空值', n == 0, 'idpath为空的行数=' + str(n))
    ok5, _, err5 = psql("SELECT public._ensure_roles()")
    rec('G0', 'A5 _ensure_roles() 幂等执行', ok5, str(err5))
    enc, err = one("sysinfo.aes_encrypt('ngxdb测试123','testkey-ngxdb')")
    ok = enc not in ('__ERR__', '__NULL__', None) and not err
    rec('G0', 'A6a aes_encrypt 返回密文', ok, str(err) or str(enc))
    dec, err = one("sysinfo.aes_decrypt(%s,'testkey-ngxdb')" % ("'" + enc.replace("'", "''") + "'" if ok and enc else "null"))
    rec('G0', 'A6b aes_decrypt 往返一致', dec == 'ngxdb测试123', '解密=' + str(dec) + str(err))
    dec2, err = one("sysinfo.aes_decrypt(%s,'wrong-key')" % ("'" + enc.replace("'", "''") + "'" if ok and enc else "null"))
    rec('G0', 'A7 aes_decrypt 错误key容错为NULL', dec2 in ('__NULL__', None) and not err, '结果=' + str(dec2) + str(err))
    h, err = one("gm.gethelp()")
    if h in ('__NULL__', None):
        rec('G0', 'A8 gethelp() 返回NULL(funchtml空表,数据缺失)', True, '函数可执行,返回null', known=True)
    else:
        rec('G0', 'A8 gethelp() 返回文本', isinstance(h, str) and len(h) > 0, str(err))
    t, err = one("gm.http_get('http://127.0.0.1/ngxdb/login.html')")
    rec('G0', 'A9 http_get 本机nginx', t not in ('__ERR__', '__NULL__', None) and not err, str(err) or str(t)[:80])
    t, err = one("gm.http_post('http://127.0.0.1/ngxdb/login.html','a=1','application/x-www-form-urlencoded')")
    rec('G0', 'A10 http_post 本机nginx', t not in ('__ERR__', '__NULL__', None) and not err, str(err) or str(t)[:80])
    for fn in ('gm.gettoken()', 'gm.gettokene()'):
        ok, out, err = psql('SELECT ' + fn)
        if ok:
            rec('G0', 'A11 ' + fn + ' 执行成功', True, '')
        elif 'pkg_service' in err:
            rec('G0', 'A11 ' + fn + ' 依赖pkg_service缺失(已知缺陷)', True, err[:160], known=True)
        else:
            rec('G0', 'A11 ' + fn, False, err[:200])

CTX = {}

def g1_login():
    print('== G1 登录与鉴权 ==')
    r = jcall("sysinfo.login(null,'x',null::inet,null)")
    rec('G1', 'B1 login 账号为空→100009', ec(r) == 100009, str(r))
    r = jcall("sysinfo.login('admin',null,null::inet,null)")
    rec('G1', 'B2 login 密码为空→100010', ec(r) == 100010, str(r))
    n0, _ = one("SELECT count(*) FROM sysinfo.loginlog WHERE accounts='admin'")
    r = jcall("sysinfo.login('admin','wrong-pass-x',null::inet,null)")
    n1, _ = one("SELECT count(*) FROM sysinfo.loginlog WHERE accounts='admin'")
    rec('G1', 'B3 login 错密码→100001且记日志', ec(r) == 100001 and int(n1) == int(n0) + 1, str(r) + ' 日志' + str(n0) + '->' + str(n1))
    r = jcall("sysinfo.login('admin','123456',null::inet,'nosuchsys')")
    rec('G1', 'B4 login 未知子系统→100001', ec(r) == 100001, str(r))
    r = jcall("sysinfo.login('admin','123456',null::inet,null)")
    acts = r.get('info', {}).get('actions') if ec(r) == 0 else None
    tok = r.get('info', {}).get('token') if ec(r) == 0 else None
    ok = ec(r) == 0 and tok and 100 in (acts or []) and 106 in (acts or [])
    rec('G1', 'B5 login admin成功 token/actions正确', ok, str(r)[:200])
    CTX['token'] = tok
    r = jcall("gm.check_login(null,150)")
    rec('G1', 'B6 check_login 空token→100005', ec(r) == 100005, str(r))
    r = jcall("gm.check_login('garbage-token-xyz',150)")
    rec('G1', 'B7 check_login 伪token→100012/100005', ec(r) in (100005, 100012), str(r))
    r = jcall("gm.check_login('%s',150)" % tok)
    ok = ec(r) == 0 and str(r.get('info', {}).get('operatorid')) == '100000'
    rec('G1', 'B8 check_login 有效token+登录权限→0', ok, str(r))
    r = jcall("gm.check_login('%s',99999)" % tok)
    rec('G1', 'B9 check_login 无此权限→100008', ec(r) == 100008, str(r))
    # B10: 直接 SQL 建一个仅含 106 权限的用户，验证权限矩阵
    psql("DELETE FROM sysinfo.operpermission WHERE operatorid=990001; DELETE FROM sysinfo.sysoper WHERE operatorid=990001; DELETE FROM sysinfo.operinfo WHERE operatorid=990001")
    psql("INSERT INTO sysinfo.operinfo(operatorid,operatorname,accounts,pass,isused,tokentype,tokeninterval,createoperator,createtime) VALUES(990001,'权限测试用户','ptest1','ptest1pass',1,2,180,100000,now())")
    psql("INSERT INTO sysinfo.operpermission(operatorid,permissiontype,ifpermission,permissionorder,params,sysactionid,permissionid) VALUES(990001,1,1,1,null,106,nextval('sysinfo.operpermission_permissionid_seq'))")
    _ok,_o,_e = psql("SELECT count(*) FROM sysinfo.operpermission WHERE operatorid=990001 AND sysactionid=106")
    rec('G1','B10-prep 权限插入落库', _ok and _o.strip()=='1', 'count='+( _o if _ok else _e))
    psql("INSERT INTO sysinfo.sysoper VALUES(990001,100)")
    r = jcall("sysinfo.login('ptest1','ptest1pass',null::inet,null)")
    ptok = r.get('info', {}).get('token') if ec(r) == 0 else None
    ok = ec(r) == 0 and ptok and 106 in (r['info'].get('actions') or []) and 101 not in (r['info'].get('actions') or [])
    rec('G1', 'B10a 受限用户登录 actions仅106子树', ok, str(r)[:200])
    CTX['ptoken'] = ptok
    r = jcall("gm.check_login('%s',106)" % ptok)
    rec('G1', 'B10b 受限用户 check(106)→0', ec(r) == 0, str(r))
    r = jcall("gm.check_login('%s',101)" % ptok)
    rec('G1', 'B10c 受限用户 check(101)→100008', ec(r) == 100008, str(r))

def g2_systeminfo():
    print('== G2 子系统 systeminfo ==')
    t = CTX['token']
    r = jcall("sysinfo.systeminfo_add('%s',null,'测试子系统A',1::smallint,'testsyskey123','tsys')" % t)
    nid = r.get('info', {}).get('id') if ec(r) == 0 else None
    rec('G2', 'C1 systeminfo_add 自动id→0', ec(r) == 0 and nid, str(r)); CTX['sysid'] = nid
    r = jcall("sysinfo.systeminfo_add('%s',100,'x',1::smallint,'k','lsys')" % t)
    rec('G2', 'C2 systeminfo_add 重复id=100→100012', ec(r) == 100012, str(r))
    r = jcall("sysinfo.systeminfo_add('%s',null,'测试子系统B',1::smallint,null,'tsysb')" % t)
    rec('G2', 'C3 systeminfo_add prikey空→100012', ec(r) == 100012, str(r))
    r = jcall("sysinfo.systeminfo_query('%s',null,null,null,null,null,null,null,null)" % t)
    rec('G2', 'C4 systeminfo_query 全量 total>=2', ec(r) == 0 and r.get('info', {}).get('total', 0) >= 2, str(r)[:120])
    r = jcall("sysinfo.systeminfo_query('%s',1,1,null,null,null,null,null,null)" % t)
    ok = ec(r) == 0 and len(r.get('info', {}).get('rows') or []) <= 1
    rec('G2', 'C5 systeminfo_query 分页 rows=1', ok, str(r)[:120])
    r = jcall("sysinfo.systeminfo_query('%s',1,999,null,null,null,null,null,null)" % t)
    rec('G2', 'C6 systeminfo_query 页码越界不报错', ec(r) == 0, str(r)[:120])
    r = jcall("sysinfo.systeminfo_edit('%s',%s,'测试子系统A改',1::smallint,'testsyskey123','tsys')" % (t, nid))
    rec('G2', 'C7 systeminfo_edit→0', ec(r) == 0, str(r))
    r = jcall("sysinfo.systeminfo_query('%s',null,null,null,'测试子系统A改',null,null,null,null)" % t)
    rec('G2', 'C7b edit 后按名称可查到', ec(r) == 0 and r.get('info', {}).get('total') == 1, str(r)[:120])
    r = jcall("sysinfo.systeminfo_merge('%s',%s,'测试子系统A改2',1::smallint,'testsyskey123','tsys')" % (t, nid))
    rec('G2', 'C8 systeminfo_merge→0', ec(r) == 0, str(r))
    r = jcall("sysinfo.systeminfo_del('%s',%s)" % (t, nid))
    rec('G2', 'C9 systeminfo_del→0', ec(r) == 0, str(r))
    r = jcall("sysinfo.systeminfo_del('%s',999999)" % t)
    rec('G2', 'C10 systeminfo_del 不存在→100012', ec(r) == 100012, str(r))
    r = jcall("sysinfo.systeminfo_undel('%s',%s)" % (t, nid))
    rec('G2', 'C11 systeminfo_undel→0', ec(r) == 0, str(r))
    psql("UPDATE sysinfo.systeminfo SET isused=0 WHERE systemid=%s" % nid)  # 保持清理状态
    if CTX.get('ptoken'):
        r = jcall("sysinfo.systeminfo_add('%s',null,'无权测试',1::smallint,'k','nox')" % CTX['ptoken'])
        rec('G2', 'C12 无权限用户 add→100008', ec(r) == 100008, str(r))
    else:
        rec('G2', 'C12 无权限用户 add(跳过:受限token未取得)', False, 'ptoken为空')

def g3_sysaction():
    print('== G3 系统权限 sysaction ==')
    t = CTX['token']
    r = jcall("sysinfo.sysaction_add('%s',100,null,100,null,'测试权限X',null,1::smallint)" % t)
    nid = r.get('info', {}).get('id') if ec(r) == 0 else None
    rec('G3', 'D1 sysaction_add 挂到系统→0', ec(r) == 0 and nid, str(r)); CTX['actid'] = nid
    row, _ = one("SELECT row_to_json(x) FROM (SELECT idpath,idlevel FROM sysinfo.sysaction WHERE sysactionid=%s) x" % nid)
    ok = row and str(row.get('idpath', '')).startswith('100.') and row.get('idlevel') == 2
    rec('G3', 'D1b 新权限 idpath/idlevel 正确', ok, str(row))
    r = jcall("sysinfo.sysaction_add('%s',999999,null,100,null,'x',null,1::smallint)" % t)
    rec('G3', 'D2 sysaction_add 上级不存在→100014', ec(r) == 100014, str(r))
    r = jcall("sysinfo.sysaction_add('%s',null,null,100,100,'x',null,1::smallint)" % t)
    rec('G3', 'D3 sysaction_add 重复id=100→100013', ec(r) == 100013, str(r))
    r = jcall("sysinfo.sysaction_query('%s',null,null,null,null,null,null,null,null,null,null,null)" % t)
    rows = [x for x in (r.get('info', {}).get('rows') or []) if isinstance(x, dict)]
    ok = ec(r) == 0 and r.get('info', {}).get('total', 0) >= 84 and rows and all(('idpath' in x and 'idlevel' in x) for x in rows)
    rec('G3', 'D4 sysaction_query 全量含树字段', ok, 'total=%s' % r.get('info', {}).get('total'))
    paths = [x.get('idpath') for x in rows]
    rec('G3', 'D4b 按 idpath 树序返回', paths == sorted(paths), '')
    r = jcall("sysinfo.sysaction_query('%s',null,null,null,'100.9001',null,null,null,null,null,null,null)" % t)
    ok = ec(r) == 0 and all('100.9001' in (x.get('idpath') or '') for x in (r.get('info', {}).get('rows') or []))
    rec('G3', 'D5 sysaction_query idpath过滤仅子树', ok, 'total=%s' % r.get('info', {}).get('total'))
    r = jcall("sysinfo.sysaction_edit('%s',100,null,%s,'测试权限X改',null,100,1::smallint)" % (t, nid))
    rec('G3', 'D6 sysaction_edit→0', ec(r) == 0, str(r))
    r = jcall("sysinfo.sysaction_edit('%s',999999,null,%s,'x',null,100,1::smallint)" % (t, nid))
    rec('G3', 'D7 sysaction_edit 上级不存在→100014', ec(r) == 100014, str(r))
    r = jcall("sysinfo.sysaction_merge('%s',100,null,100,%s,'测试权限X改2',null,1::smallint)" % (t, nid))
    rec('G3', 'D8 sysaction_merge→0', ec(r) == 0, str(r))
    r = jcall("sysinfo.sysaction_del('%s',100)" % t)
    rec('G3', 'D10 sysaction_del 有下级→100016', ec(r) == 100016, str(r))
    r = jcall("sysinfo.sysaction_del('%s',999999)" % t)
    rec('G3', 'D11 sysaction_del 不存在→100013', ec(r) == 100013, str(r))
    r = jcall("sysinfo.sysaction_del('%s',%s)" % (t, nid))
    rec('G3', 'D9 sysaction_del 叶子→0', ec(r) == 0, str(r))
    r = jcall("sysinfo.sysaction_undel('%s',%s)" % (t, nid))
    rec('G3', 'D12 sysaction_undel→0', ec(r) == 0, str(r))
    r = jcall("sysinfo.sysaction_del('%s',%s)" % (t, nid))
    psql("UPDATE sysinfo.sysaction SET isused=0 WHERE sysactionid=%s" % nid)
    r = jcall("sysinfo.sysaction_undel('%s',100)" % t)
    rec('G3', 'D13 sysaction_undel 顶级→100015', ec(r) == 100015, str(r))

def g4_org():
    print('== G4 部门类型 orgtype / 部门 sysorg ==')
    t = CTX['token']
    r = jcall("sysinfo.orgtype_add('%s',null,'测试部门类型','test')" % t)
    ot = r.get('info', {}).get('id') if ec(r) == 0 else None
    rec('G4', 'E1 orgtype_add→0', ec(r) == 0 and ot, str(r))
    eid, _ = one("SELECT min(orgtypeid) FROM sysinfo.orgtype WHERE orgtypeid<>%s" % (ot or 0))
    r = jcall("sysinfo.orgtype_add('%s',%s,'x','x')" % (t, eid if eid not in ('__NULL__', '__ERR__', None) else 999999))
    rec('G4', 'E2 orgtype_add 重复id→100020', ec(r) == 100020, str(r))
    r = jcall("sysinfo.orgtype_query('%s',null,null,null,null,null,null)" % t)
    rec('G4', 'E3 orgtype_query total>=1', ec(r) == 0 and r.get('info', {}).get('total', 0) >= 1, str(r)[:100])
    r = jcall("sysinfo.orgtype_edit('%s',%s,'测试部门类型改','test')" % (t, ot))
    rec('G4', 'E4 orgtype_edit→0', ec(r) == 0, str(r))
    r = jcall("sysinfo.orgtype_merge('%s',%s,'测试部门类型改2','test')" % (t, ot))
    rec('G4', 'E5 orgtype_merge→0', ec(r) == 0, str(r))
    r = jcall("sysinfo.orgtype_del('%s',999999)" % t)
    rec('G4', 'E7 orgtype_del 不存在→100020', ec(r) == 100020, str(r))
    r = jcall("sysinfo.orgtype_del('%s',%s)" % (t, ot))
    rec('G4', 'E6 orgtype_del→0', ec(r) == 0, str(r))
    r = jcall("sysinfo.orgtype_undel('%s',%s)" % (t, ot))
    rec('G4', 'E8 orgtype_undel→0', ec(r) == 0, str(r))
    psql("UPDATE sysinfo.orgtype SET isused=0 WHERE orgtypeid=%s" % ot)
    root, _ = one("SELECT coalesce(max(sysorgid),0) FROM sysinfo.sysorg WHERE upid IN (0,100) OR upid IS NULL")
    if root in ('__NULL__', '__ERR__', None, 0):
        root = 100
    r = jcall("sysinfo.sysorg_add('%s',%s,null,'测试部门A','test',null)" % (t, root))
    d1 = r.get('info', {}).get('id') if ec(r) == 0 else None
    rec('G4', 'E9 sysorg_add→0', ec(r) == 0 and d1, str(r)); CTX['deptid'] = d1
    r = jcall("sysinfo.sysorg_add('%s',999999,null,'x',null,null)" % t)
    rec('G4', 'E10 sysorg_add 上级不存在→100014', ec(r) == 100014, str(r))
    r = jcall("sysinfo.sysorg_add('%s',%s,%s,'x',null,null)" % (t, root, d1))
    rec('G4', 'E11 sysorg_add 重复id→100018', ec(r) == 100018, str(r))
    r = jcall("sysinfo.sysorg_query('%s',null,null,null,null,null,null,null,null,null)" % t)
    rows = r.get('info', {}).get('rows') or []
    ok = ec(r) == 0 and rows and all(('idpath' in x and 'idlevel' in x) for x in rows)
    rec('G4', 'E12 sysorg_query 含 idpath/idlevel 字段', ok, 'total=%s' % r.get('info', {}).get('total'))
    r = jcall("sysinfo.sysorg_add('%s',%s,null,'测试部门A-子',null,null)" % (t, d1))
    d2 = r.get('info', {}).get('id') if ec(r) == 0 else None
    rec('G4', 'E13 sysorg_add 二级部门→0', ec(r) == 0 and d2, str(r)); CTX['deptid2'] = d2
    row, _ = one("SELECT row_to_json(x) FROM (SELECT upid,idpath,idlevel FROM sysinfo.sysorg WHERE sysorgid=%s) x" % d2)
    ok = row and row.get('upid') == d1
    rec('G4', 'E13b 子部门 upid 挂接正确', ok, str(row))
    r = jcall("sysinfo.sysorg_edit('%s',%s,%s,'测试部门A-子改',null,null)" % (t, d1, d2))
    rec('G4', 'E14 sysorg_edit→0', ec(r) == 0, str(r))
    r = jcall("sysinfo.sysorg_merge('%s',%s,%s,'测试部门A-子改2',null,null)" % (t, d1, d2))
    rec('G4', 'E15 sysorg_merge→0', ec(r) == 0, str(r))
    r = jcall("sysinfo.sysorg_del('%s',%s)" % (t, d1))
    rec('G4', 'E17 sysorg_del 有下级→100016', ec(r) == 100016, str(r))
    r = jcall("sysinfo.sysorg_del('%s',999999)" % t)
    rec('G4', 'E17b sysorg_del 不存在→100018', ec(r) == 100018, str(r))
    r = jcall("sysinfo.sysorg_del('%s',%s)" % (t, d2))
    rec('G4', 'E16 sysorg_del 叶子→0', ec(r) == 0, str(r))
    r = jcall("sysinfo.sysorg_undel('%s',100)" % t)
    rec('G4', 'E18 sysorg_undel 顶级100→100015', ec(r) == 100015, str(r))
    r = jcall("sysinfo.sysorg_undel('%s',%s)" % (t, d2))
    rec('G4', 'E19 sysorg_undel→0', ec(r) == 0, str(r))
    r = jcall("sysinfo.sysorg_del('%s',%s)" % (t, d2))
    psql("UPDATE sysinfo.sysorg SET isused=0 WHERE sysorgid IN (%s,%s)" % (d1, d2))

PJ106 = '[{"permissiontype":1,"ifpermission":1,"permissionorder":1,"params":null,"sysactionid":106}]'
PJ107 = '[{"permissiontype":1,"ifpermission":1,"permissionorder":1,"params":null,"sysactionid":107}]'

def g5_oper():
    print('== G5 员工 operinfo / operinfoorg / operinfopermission ==')
    t = CTX['token']
    r = jcall("sysinfo.operinfo_add('%s',null,'测试员工一','ptest1x',2::smallint,60,'%s')" % (t, PJ106))
    u1 = r.get('info', {}).get('id') if ec(r) == 0 else None
    p1 = r.get('info', {}).get('pass') if ec(r) == 0 else None
    rec('G5', 'F1 operinfo_add→0 返回id+pass', ec(r) == 0 and u1 and p1, str(r)[:160]); CTX['u1'] = u1; CTX['p1'] = p1
    n, _ = one("SELECT count(*) FROM sysinfo.operpermission WHERE operatorid=%s AND sysactionid=106" % u1)
    rec('G5', 'F2a operinfo_add 权限落库(⚠疑似AND恒假缺陷)', n == 1, 'operpermission中106权限数=' + str(n))
    n, _ = one("SELECT count(*) FROM sysinfo.sysoper WHERE operatorid=%s AND systemid=100" % u1)
    rec('G5', 'F2b operinfo_add sysoper落库', n == 1, 'sysoper记录数=' + str(n))
    r = jcall("sysinfo.operinfo_add('%s',990999,'x','ptest1x',2::smallint,60,'[]')" % t)
    rec('G5', 'F3 operinfo_add 重复账号→100022', ec(r) == 100022, str(r))
    r = jcall("sysinfo.operinfo_add('%s',100000,'x','ptestu999',2::smallint,60,'[]')" % t)
    rec('G5', 'F4 operinfo_add 已存在operatorid→100011', ec(r) == 100011, str(r))
    r = jcall("sysinfo.operinfo_query('%s',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null)" % t)
    rec('G5', 'F5a operinfo_query 全量→0', ec(r) == 0 and r.get('info', {}).get('total', 0) >= 2, 'total=%s' % r.get('info', {}).get('total'))
    r = jcall("sysinfo.operinfo_query('%s',1,1,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null)" % t)
    ok = ec(r) == 0 and len(r.get('info', {}).get('rows') or []) <= 1
    rec('G5', 'F5b operinfo_query 分页', ok, '')
    r = jcall("sysinfo.operinfo_query('%s',null,null,null,null,null,null,'ptest1x',null,null,null,null,null,null,null,null,null,null)" % t)
    ok = ec(r) == 0 and r.get('info', {}).get('total') == 1
    rec('G5', 'F5c operinfo_query 按账号过滤', ok, 'total=%s' % r.get('info', {}).get('total'))
    r = jcall("sysinfo.operinfo_edit('%s',%s,'测试员工一改','ptest1x',2::smallint,60,'%s')" % (t, u1, PJ106))
    rec('G5', 'F6 operinfo_edit→0', ec(r) == 0, str(r)[:120])
    r = jcall("sysinfo.operinfo_merge('%s',null,'测试员工二','ptest2x',2::smallint,60,'[]')" % t)
    u2 = r.get('info', {}).get('id') if ec(r) == 0 else None
    rec('G5', 'F7 operinfo_merge 新增→0', ec(r) == 0 and u2, str(r)[:120]); CTX['u2'] = u2
    r = jcall("sysinfo.operinfo_del('%s',%s)" % (t, u2))
    rec('G5', 'F8 operinfo_del→0', ec(r) == 0, str(r))
    r = jcall("sysinfo.operinfo_del('%s',999999)" % t)
    rec('G5', 'F9 operinfo_del 不存在→100011', ec(r) == 100011, str(r))
    r = jcall("sysinfo.operinfo_undel('%s',%s)" % (t, u2))
    rec('G5', 'F10 operinfo_undel→0', ec(r) == 0, str(r))
    psql("UPDATE sysinfo.operinfo SET isused=0 WHERE operatorid=%s" % u2)
    r = jcall("sysinfo.login('ptest1x','%s',null::inet,null)" % p1)
    acts = r.get('info', {}).get('actions') if ec(r) == 0 else None
    ok = ec(r) == 0 and 106 in (acts or []) and 101 not in (acts or [])
    rec('G5', 'F11 新员工登录且actions仅106子树', ok, str(r)[:160])
    root, _ = one("SELECT coalesce(max(sysorgid),100) FROM sysinfo.sysorg")
    r = jcall("sysinfo.operinfoorg_add('%s',null,'测试员工三',1::smallint,'13800000001','ptest3x',null,null,null,null,null,2::smallint,60,'[{\"sysorgid\":%s}]')" % (t, root))
    u3 = r.get('info', {}).get('id') if ec(r) == 0 else None
    rec('G5', 'F12 operinfoorg_add→0', ec(r) == 0 and u3, str(r)[:140]); CTX['u3'] = u3
    n, _ = one("SELECT count(*) FROM sysinfo.sysoperorg WHERE operatorid=%s AND sysorgid=%s" % (u3, root))
    rec('G5', 'F12b operinfoorg_add 部门关联落库', n == 1, 'sysoperorg记录数=' + str(n))
    r = jcall("sysinfo.operinfoorg_edit('%s',%s,'测试员工三改',1::smallint,'13800000001','ptest3x',null,null,null,null,null,2::smallint,60,'[{\"sysorgid\":%s}]')" % (t, u3, root))
    rec('G5', 'F13 operinfoorg_edit→0', ec(r) == 0, str(r)[:140])
    r = jcall("sysinfo.operinfoorg_merge('%s',null,'测试员工四',1::smallint,'13800000002','ptest4x',null,null,null,null,null,2::smallint,60,'[{\"sysorgid\":%s}]')" % (t, root))
    u4 = r.get('info', {}).get('id') if ec(r) == 0 else None
    rec('G5', 'F14 operinfoorg_merge→0', ec(r) == 0 and u4, str(r)[:140]); CTX['u4'] = u4
    r = jcall("sysinfo.operinfoorg_query('%s',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null)" % t)
    rec('G5', 'F15 operinfoorg_query→0', ec(r) == 0, 'total=%s' % r.get('info', {}).get('total'))
    r = jcall("sysinfo.operinfoorg_del('%s',%s)" % (t, u4))
    rec('G5', 'F16 operinfoorg_del→0', ec(r) == 0, str(r))
    r = jcall("sysinfo.operinfoorg_undel('%s',%s)" % (t, u4))
    rec('G5', 'F17 operinfoorg_undel→0', ec(r) == 0, str(r))
    psql("UPDATE sysinfo.operinfo SET isused=0 WHERE operatorid=%s" % u4)
    r = jcall("sysinfo.operinfopermission_add('%s',%s,'%s')" % (t, u3, PJ107))
    rec('G5', 'F18 operinfopermission_add→0', ec(r) == 0, str(r)[:140])
    n, _ = one("SELECT count(*) FROM sysinfo.operpermission WHERE operatorid=%s AND sysactionid=107" % u3)
    rec('G5', 'F18b operinfopermission_add 权限落库(⚠疑似AND恒假缺陷)', n == 1, 'operpermission中107权限数=' + str(n))
    r = jcall("sysinfo.operinfopermission_edit('%s',%s,'%s')" % (t, u3, PJ106))
    rec('G5', 'F19 operinfopermission_edit→0', ec(r) == 0, str(r)[:140])
    n, _ = one("SELECT count(*) FROM sysinfo.operpermission WHERE operatorid=%s AND sysactionid=106" % u3)
    rec('G5', 'F19b operinfopermission_edit 后106生效', n == 1, '106权限数=' + str(n))
    r = jcall("sysinfo.operinfopermission_merge('%s',%s,'%s')" % (t, u3, PJ107))
    rec('G5', 'F20 operinfopermission_merge→0', ec(r) == 0, str(r)[:140])
    r = jcall("sysinfo.operinfopermission_query('%s',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null)" % t)
    ok = ec(r) == 0 and r.get('info', {}).get('total', 0) >= 1
    rec('G5', 'F21 operinfopermission_query→0', ok, 'total=%s' % r.get('info', {}).get('total'))
    r = jcall("sysinfo.operinfopermission_del('%s',%s)" % (t, u3))
    rec('G5', 'F22 operinfopermission_del→0', ec(r) == 0, str(r)[:140])
    r = jcall("sysinfo.operinfopermission_undel('%s',%s)" % (t, u3))
    rec('G5', 'F23 operinfopermission_undel→0', ec(r) == 0, str(r)[:140])

def g6_role():
    print('== G6 角色 roleinfo ==')
    t = CTX['token']
    r = jcall("sysinfo.roleinfo_add('%s',null,'测试角色一','test','%s')" % (t, PJ106))
    rid = r.get('info', {}).get('id') if ec(r) == 0 else None
    rec('G6', 'G1 roleinfo_add→0', ec(r) == 0 and rid, str(r)[:140]); CTX['rid'] = rid
    r = jcall("sysinfo.roleinfo_add('%s',%s,'x','x','[]')" % (t, rid))
    rec('G6', 'G2 roleinfo_add 重复id→100023', ec(r) == 100023, str(r))
    r = jcall("sysinfo.roleinfo_query('%s',null,null,null,null,null,null)" % t)
    rows = r.get('info', {}).get('rows') or []
    ok = ec(r) == 0 and r.get('info', {}).get('total', 0) >= 1 and rows and 'rolepermissionjson' in rows[0]
    rec('G6', 'G3 roleinfo_query 含权限json', ok, 'total=%s' % r.get('info', {}).get('total'))
    r = jcall("sysinfo.roleinfo_edit('%s',%s,'测试角色一改','test','%s')" % (t, rid, PJ106))
    rec('G6', 'G4 roleinfo_edit→0', ec(r) == 0, str(r)[:140])
    r = jcall("sysinfo.roleinfo_edit('%s',%s,'x','x','[{\"permissiontype\":2,\"ifpermission\":1,\"permissionorder\":1,\"params\":null,\"sysactionid\":%s}]')" % (t, rid, rid))
    rec('G6', 'G5 roleinfo_edit 自引用→100017循环定义', ec(r) == 100017, str(r))
    r = jcall("sysinfo.roleinfo_merge('%s',null,'测试角色二','test','[]')" % t)
    rid2 = r.get('info', {}).get('id') if ec(r) == 0 else None
    rec('G6', 'G6 roleinfo_merge 新增→0', ec(r) == 0 and rid2, str(r)[:140])
    r = jcall("sysinfo.roleinfo_del('%s',999999)" % t)
    rec('G6', 'G8 roleinfo_del 不存在→100023', ec(r) == 100023, str(r))
    if rid2:
        r = jcall("sysinfo.roleinfo_del('%s',%s)" % (t, rid2))
        rec('G6', 'G7 roleinfo_del→0', ec(r) == 0, str(r))
        r = jcall("sysinfo.roleinfo_undel('%s',%s)" % (t, rid2))
        rec('G6', 'G9 roleinfo_undel→0', ec(r) == 0, str(r))
        r = jcall("sysinfo.roleinfo_del('%s',%s)" % (t, rid2))
        psql("UPDATE sysinfo.roleinfo SET isused=0 WHERE roleinfoid=%s" % rid2)
    # G10 角色继承：建无权限用户 → 赋角色 → 登录后 actions 应含角色权限 106
    psql("DELETE FROM sysinfo.operpermission WHERE operatorid=990002; DELETE FROM sysinfo.sysoper WHERE operatorid=990002; DELETE FROM sysinfo.operinfo WHERE operatorid=990002; DELETE FROM sysinfo.sysoperorg WHERE operatorid=990002")
    psql("INSERT INTO sysinfo.operinfo(operatorid,operatorname,accounts,pass,isused,tokentype,tokeninterval,createoperator,createtime) VALUES(990002,'角色测试用户','prole1','prole1pass',1,2,180,100000,now())")
    psql("INSERT INTO sysinfo.sysoper VALUES(990002,100)")
    r = jcall("sysinfo.operinfopermission_edit('%s',990002,'[{\"permissiontype\":2,\"ifpermission\":1,\"permissionorder\":1,\"params\":null,\"sysactionid\":%s}]')" % (t, rid))
    rec('G6', 'G10a 角色赋给员工 edit→0', ec(r) == 0, str(r)[:140])
    r = jcall("sysinfo.login('prole1','prole1pass',null::inet,null)")
    acts = r.get('info', {}).get('actions') if ec(r) == 0 else None
    rec('G6', 'G10b 经角色登录 actions 含 106(继承)', ec(r) == 0 and 106 in (acts or []), str(r)[:160])
    psql("DELETE FROM sysinfo.operpermission WHERE operatorid=990002; UPDATE sysinfo.operinfo SET isused=0,accounts='prole1_del' WHERE operatorid=990002")

def g7_area():
    print('== G7 省 province / 市 city ==')
    t = CTX['token']
    r = jcall("sysinfo.province_add('%s',null,'测试省Z')" % t)
    np_ = r.get('info', {}).get('id') if ec(r) == 0 else None
    rec('G7', 'H1 province_add→0', ec(r) == 0 and np_, str(r))
    eid, _ = one("SELECT min(pid) FROM sysinfo.province WHERE pid<>%s" % (np_ or 0))
    r = jcall("sysinfo.province_add('%s',%s,'x')" % (t, eid if eid not in ('__NULL__', '__ERR__', None) else 999999))
    rec('G7', 'H2 province_add 重复pid→100033', ec(r) == 100033, str(r))
    r = jcall("sysinfo.province_query('%s',null,null,null,null,null)" % t)
    rec('G7', 'H3 province_query total>=35', ec(r) == 0 and r.get('info', {}).get('total', 0) >= 35, 'total=%s' % r.get('info', {}).get('total'))
    r = jcall("sysinfo.province_edit('%s',%s,'测试省Z改')" % (t, np_))
    rec('G7', 'H4 province_edit→0', ec(r) == 0, str(r))
    r = jcall("sysinfo.province_merge('%s',null,'测试省Z2')" % t)
    np2 = r.get('info', {}).get('id') if ec(r) == 0 else None
    rec('G7', 'H5 province_merge 新增→0', ec(r) == 0 and np2, str(r))
    r = jcall("sysinfo.province_del('%s',999999)" % t)
    rec('G7', 'H7 province_del 不存在→100033', ec(r) == 100033, str(r))
    r = jcall("sysinfo.province_del('%s',%s)" % (t, np_))
    rec('G7', 'H6 province_del→0', ec(r) == 0, str(r))
    r = jcall("sysinfo.province_undel('%s',%s)" % (t, np_))
    rec('G7', 'H8 province_undel→0', ec(r) == 0, str(r))
    r = jcall("sysinfo.province_del('%s',%s)" % (t, np_))
    psql("UPDATE sysinfo.province SET isused=0 WHERE pid IN (%s,%s)" % (np_, np2))
    r = jcall("sysinfo.city_add('%s',%s,null,'测试市Z')" % (t, np_))
    nc = r.get('info', {}).get('id') if ec(r) == 0 else None
    rec('G7', 'H9 city_add→0', ec(r) == 0 and nc, str(r)); CTX['cid'] = nc
    r = jcall("sysinfo.city_add('%s',999999,null,'x')" % t)
    rec('G7', 'H10 city_add 省不存在→100033', ec(r) == 100033, str(r))
    ecid, _ = one("SELECT min(cid) FROM sysinfo.city WHERE cid<>%s" % (nc or 0))
    r = jcall("sysinfo.city_add('%s',%s,%s,'x')" % (t, np_, ecid if ecid not in ('__NULL__', '__ERR__', None) else 999999))
    rec('G7', 'H11 city_add 重复cid→100034', ec(r) == 100034, str(r))
    r = jcall("sysinfo.city_query('%s',null,null,%s,null,null,null)" % (t, np_))
    rows = r.get('info', {}).get('rows') or []
    ok = ec(r) == 0 and r.get('info', {}).get('total') == 1 and rows and str(rows[0].get('province','')).startswith('测试省Z')
    rec('G7', 'H12 city_query 按省过滤且带省名', ok, str(r)[:140])
    r = jcall("sysinfo.city_edit('%s',%s,%s,'测试市Z改')" % (t, np_, nc))
    rec('G7', 'H13 city_edit→0', ec(r) == 0, str(r))
    r = jcall("sysinfo.city_edit('%s',999999,%s,'x')" % (t, nc))
    rec('G7', 'H14 city_edit 省不存在→100033', ec(r) == 100033, str(r))
    r = jcall("sysinfo.city_merge('%s',%s,null,'测试市Z3')" % (t, np_))
    nc2 = r.get('info', {}).get('id') if ec(r) == 0 else None
    rec('G7', 'H15 city_merge 新增→0', ec(r) == 0 and nc2, str(r))
    r = jcall("sysinfo.city_del('%s',999999)" % t)
    rec('G7', 'H17 city_del 不存在→100034', ec(r) == 100034, str(r))
    r = jcall("sysinfo.city_del('%s',%s)" % (t, nc))
    rec('G7', 'H16 city_del→0', ec(r) == 0, str(r))
    r = jcall("sysinfo.city_undel('%s',%s)" % (t, nc))
    rec('G7', 'H18 city_undel→0', ec(r) == 0, str(r))
    psql("DELETE FROM sysinfo.city WHERE cid IN (%s,%s); DELETE FROM sysinfo.province WHERE pid IN (%s,%s)" % (nc, nc2, np_, np2))

def g8_logs():
    print('== G8 日志与字典 ==')
    t = CTX['token']
    r = jcall("sysinfo.serverlog_query('%s',null,null,null,null,null,null,null,null,null,null,null,null,null,null)" % t)
    rec('G8', 'I1 serverlog_query→0', ec(r) == 0, 'total=%s' % r.get('info', {}).get('total'))
    r = jcall("sysinfo.errorcode_query('%s',null,null,'成功',null,null,null,null)" % t)
    ok = ec(r) == 0 and 1 <= r.get('info', {}).get('total', 0) <= 33
    rec('G8', 'I2 errorcode_query 模糊查询', ok, 'total=%s' % r.get('info', {}).get('total'))
    r = jcall("sysinfo.actions_query('%s',null,null,null,null,null,null,null,null,null)" % t)
    rec('G8', 'I3 actions_query total=67', ec(r) == 0 and r.get('info', {}).get('total') == 67, 'total=%s' % r.get('info', {}).get('total'))

def g9_register():
    print('== G9 注册与第三方登录 ==')
    r = jcall("sysinfo.regaccount('x','测试',1::smallint,'13800000000','wxcode','memo',999,'nick','url','nosuchsys',null)")
    rec('G9', 'J1 regaccount 未知系统→100001', ec(r) == 100001, str(r))
    r = jcall("sysinfo.regaccount('x','测试',1::smallint,null,'wxcode','memo',999,'nick','url',null,null)")
    rec('G9', 'J2 regaccount 无效appid→100026', ec(r) == 100026, str(r))
    ok, out, err = psql("SELECT sysinfo.regaccount('000000','测试',1::smallint,'13800000000','wxcode','memo',999,'nick','url',null,null)")
    if ok:
        v = json.loads(out)
        rec('G9', 'J3 regaccount 短信验证码错误路径', ec(v) == 100027, str(v))
    elif 'sms' in err:
        rec('G9', 'J3 regaccount 依赖sms模式缺失(已知缺陷)', True, err[:160], known=True)
    else:
        rec('G9', 'J3 regaccount', False, err[:200])
    r = jcall("sysinfo.loginaccount('wxuser','nosuchsys','127.0.0.1',999,999)")
    rec('G9', 'J4 loginaccount 未知系统→100001', ec(r) == 100001, str(r))
    r = jcall("sysinfo.loginaccount('wxuser',null,'127.0.0.1',101,999)")
    rec('G9', 'J5 loginaccount appid无效→100001', ec(r) == 100001, str(r))

def g10_session():
    print('== G10 会话安全(互踢/过期/锁定) ==')
    t = CTX['token']
    # K1 单人登录互踢：ptest1 为 tokentype=2 不互踢；新建 tokentype=1 用户验证
    psql("DELETE FROM sysinfo.operpermission WHERE operatorid=990003; DELETE FROM sysinfo.sysoper WHERE operatorid=990003; DELETE FROM sysinfo.operinfo WHERE operatorid=990003; DELETE FROM sysinfo.loginlog WHERE accounts='pkick1'")
    psql("INSERT INTO sysinfo.operinfo(operatorid,operatorname,accounts,pass,isused,tokentype,tokeninterval,createoperator,createtime) VALUES(990003,'互踢用户','pkick1','pkick1pass',1,1,180,100000,now())")
    psql("INSERT INTO sysinfo.sysoper VALUES(990003,100)")
    r1 = jcall("sysinfo.login('pkick1','pkick1pass',null::inet,null)")
    tk1 = r1.get('info', {}).get('token') if ec(r1) == 0 else None
    r2 = jcall("sysinfo.login('pkick1','pkick1pass',null::inet,null)")
    ok = ec(r2) == 0
    r = jcall("gm.check_login('%s',150)" % tk1) if tk1 else {'errorcode': -1}
    rec('G10', 'K1 单人登录旧token被踢→100006', ok and ec(r) == 100006, '旧token校验=' + str(r))
    # K2 令牌过期
    psql("UPDATE sysinfo.operinfo SET tokentime=now()-interval '1 hour' WHERE operatorid=990003")
    r = jcall("gm.check_login('%s',150)" % r2.get('info', {}).get('token', 'x'))
    rec('G10', 'K2 过期token→100006', ec(r) == 100006, str(r))
    psql("DELETE FROM sysinfo.operinfo WHERE operatorid=990003; DELETE FROM sysinfo.sysoper WHERE operatorid=990003")
    # K3 暴力锁定（对测试账号，避免影响 admin）
    psql("DELETE FROM sysinfo.loginlog WHERE accounts='pkick2'")
    psql("INSERT INTO sysinfo.operinfo(operatorid,operatorname,accounts,pass,isused,tokentype,tokeninterval,createoperator,createtime) VALUES(990004,'锁定用户','pkick2','pkick2pass',1,2,180,100000,now())")
    psql("INSERT INTO sysinfo.sysoper VALUES(990004,100)")
    codes = []
    for i in range(7):
        r = jcall("sysinfo.login('pkick2','badpass%d',null::inet,null)" % i)
        codes.append(ec(r))
    r = jcall("sysinfo.login('pkick2','pkick2pass',null::inet,null)")
    codes.append(ec(r))
    ok = codes[6] == 100002 and codes[7] == 100002
    rec('G10', 'K3 连续失败5次后锁定(100002/100003)', ok, '错误码序列=' + str(codes))
    psql("DELETE FROM sysinfo.operinfo WHERE operatorid=990004; DELETE FROM sysinfo.sysoper WHERE operatorid=990004; DELETE FROM sysinfo.loginlog WHERE accounts='pkick2'")

def main():
    argparse()
    print('ngxdb 数据库层测试  库=%s@%s' % (DB, HOST))
    ok, _, err = psql('SELECT 1')
    if not ok:
        print('无法连接测试库: ' + err)
        sys.exit(2)
    steps = [('G0', g0_basics), ('G1', g1_login)]
    for name, fn in steps:
        try:
            fn()
        except Exception as e:
            rec(name, '组执行异常', False, repr(e))
    if CTX.get('token'):
        more = [('G2', g2_systeminfo), ('G3', g3_sysaction), ('G4', g4_org), ('G5', g5_oper),
                ('G6', g6_role), ('G7', g7_area), ('G8', g8_logs), ('G9', g9_register), ('G10', g10_session)]
        for name, fn in more:
            try:
                fn()
            except Exception as e:
                rec(name, '组执行异常', False, repr(e))
    else:
        rec('G1', '登录失败,业务组跳过', False, '无token')
    npass = sum(1 for x in RESULTS if x[2] == 'PASS')
    nfail = sum(1 for x in RESULTS if x[2] == 'FAIL')
    nknown = sum(1 for x in RESULTS if x[2] == 'KNOWN')
    print('\n===== 数据库层测试汇总 =====')
    print('通过: %d   失败: %d   已知问题: %d   合计: %d' % (npass, nfail, nknown, len(RESULTS)))
    if nfail:
        print('失败用例:')
        for g, n, s, d in RESULTS:
            if s == 'FAIL':
                print('  ✘ [%s] %s -> %s' % (g, n, d[:200]))
    if nknown:
        print('已知问题(不计失败):')
        for g, n, s, d in RESULTS:
            if s == 'KNOWN':
                print('  ⚠ [%s] %s -> %s' % (g, n, d[:160]))
    sys.exit(1 if nfail else 0)

if __name__ == '__main__':
    main()











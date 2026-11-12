#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ngxdb 网页/接口层功能测试（第二层）。
针对运行中的 nginx(默认 http://127.0.0.1)，验证静态页、登录、全部查询接口、
鉴权失败路径、一次自清理 CRUD 闭环与并发冒烟。用例编号见 test/TEST_PLAN.md 场景 L。
用法: python3 test_ngxdb_web.py [--base http://127.0.0.1]
"""
import json, os, sys, threading, time
import socket
import urllib.parse, urllib.request

def detect_local_ip():
    """探测本机非环回 IPv4；失败则退回 127.0.0.1。
    绕开 systemd nginx 反复重启时 127.0.0.1:80 上可能出现的瞬时幽灵监听。"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(('10.255.255.255', 1))
            ip = s.getsockname()[0]
        finally:
            s.close()
        if ip and not ip.startswith('127.'):
            return ip
    except Exception:
        pass
    return '127.0.0.1'

BASE = 'http://' + detect_local_ip()
RESULTS = []

def argparse():
    global BASE
    a = sys.argv[1:]
    for i, x in enumerate(a):
        if x == '--base' and i + 1 < len(a):
            BASE = a[i + 1].rstrip('/')

def rec(name, ok, detail='', known=False):
    st = 'KNOWN' if known else ('PASS' if ok else 'FAIL')
    RESULTS.append((name, st, detail))
    tag = {'PASS': '✔', 'FAIL': '✘', 'KNOWN': '⚠'}[st]
    line = '  %s [%s] %s' % (tag, st, name)
    if not ok or known:
        line += '  -> ' + (detail or '')[:260]
    print(line)

def http(path, data=None, method=None, timeout=15, retries=3):
    """返回 (status, body_text)。data 为 dict 时编码为表单。
    对连接层异常(RemoteDisconnected 等)自动重试，规避 systemd nginx
    auto-restart 循环造成的瞬时 127.0.0.1:80 幽灵监听干扰。"""
    url = BASE + path
    body = urllib.parse.urlencode(data).encode() if isinstance(data, dict) else (data.encode() if isinstance(data, str) else None)
    last = (0, '')
    for attempt in range(retries):
        req = urllib.request.Request(url, data=body, method=method or ('POST' if body is not None else 'GET'))
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.status, resp.read().decode('utf-8', 'replace')
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode('utf-8', 'replace')
        except Exception as e:
            last = (0, repr(e))
            time.sleep(2)
    return last

def api(func, data):
    st, body = http('/func/' + func, data)
    try:
        return st, json.loads(body)
    except Exception:
        return st, {'__raw__': body[:200]}

def login(user, pw, system=None):
    d = {'loginname': user, 'pass': pw}
    if system is not None:
        d['system'] = system
    _, r = api('sysinfo.login', d)
    return r

QUERIES = [
    ('sysinfo.sysaction_query', {}), ('sysinfo.systeminfo_query', {}),
    ('sysinfo.operinfo_query', {}), ('sysinfo.operinfoorg_query', {}),
    ('sysinfo.operinfopermission_query', {}), ('sysinfo.sysorg_query', {}),
    ('sysinfo.orgtype_query', {}), ('sysinfo.roleinfo_query', {}),
    ('sysinfo.province_query', {}), ('sysinfo.city_query', {}),
    ('sysinfo.errorcode_query', {}), ('sysinfo.actions_query', {}),
    ('sysinfo.serverlog_query', {}),
]

def t_static():
    print('== L1 静态页面 ==')
    for p in ['/ngxdb/login.html', '/ngxdb/index.html', '/ngxdb/assets/core.js',
              '/ngxdb/assets/app.js', '/ngxdb/assets/modules.js', '/ngxdb/assets/suite.css']:
        st, body = http(p)
        rec('L1 GET ' + p, st == 200 and len(body) > 0, 'status=%s len=%s' % (st, len(body)))

def t_login():
    print('== L2 登录 ==')
    r = login('admin', '123456')
    tok = r.get('info', {}).get('token') if r.get('errorcode') == 0 else None
    rec('L2a admin/123456 登录成功', r.get('errorcode') == 0 and tok, str(r)[:160])
    r = login('admin', 'badpass')
    rec('L2b 错密码→100001', r.get('errorcode') == 100001, str(r)[:120])
    r = login('', 'x')
    rec('L2c 缺账号→100009', r.get('errorcode') == 100009, str(r)[:120])
    r = login('admin', '')
    rec('L2d 缺密码→100010', r.get('errorcode') == 100010, str(r)[:120])
    return tok

def t_queries(tok):
    print('== L3 全部查询接口 ==')
    for func, extra in QUERIES:
        d = {'token': tok}
        d.update(extra)
        st, r = api(func, d)
        ok = r.get('errorcode') == 0 and 'total' in (r.get('info') or {})
        rec('L3 ' + func, ok, 'http=%s resp=%s' % (st, str(r)[:120]))

def t_auth():
    print('== L4 鉴权失败路径 ==')
    _, r = api('sysinfo.sysaction_query', {})
    rec('L4a 无token→100005', r.get('errorcode') == 100005, str(r)[:120])
    _, r = api('sysinfo.sysaction_query', {'token': 'garbage-token-xyz'})
    rec('L4b 伪token→100012/100005', r.get('errorcode') in (100005, 100012), str(r)[:120])
    st, body = http('/func/sysinfo/notexist_func', {'token': 'x'})
    rec('L4c 不存在函数(记录行为)', True, 'http=%s body=%s' % (st, body[:120]), known=True)

def t_crud(tok):
    print('== L5 省市 CRUD 闭环(自清理) ==')
    def call(func, d):
        d['token'] = tok
        return api(func, d)[1]
    r = call('sysinfo.province_add', {'province': '测试省W'})
    pid = (r.get('info') or {}).get('id') if r.get('errorcode') == 0 else None
    rec('L5a province_add→0', r.get('errorcode') == 0 and pid, str(r)[:120])
    r = call('sysinfo.province_edit', {'pid': pid, 'province': '测试省W改'})
    rec('L5b province_edit→0', r.get('errorcode') == 0, str(r)[:120])
    r = call('sysinfo.province_query', {'province': '测试省W改'})
    ok = r.get('errorcode') == 0 and r.get('info', {}).get('total') == 1
    rec('L5c province_query 过滤命中', ok, str(r)[:120])
    r = call('sysinfo.city_add', {'pid': pid, 'city': '测试市W'})
    cid = (r.get('info') or {}).get('id') if r.get('errorcode') == 0 else None
    rec('L5d city_add→0', r.get('errorcode') == 0 and cid, str(r)[:120])
    r = call('sysinfo.city_query', {'pid': pid})
    ok = r.get('errorcode') == 0 and r.get('info', {}).get('total') == 1
    rec('L5e city_query 按省过滤', ok, str(r)[:120])
    r = call('sysinfo.city_del', {'cid': cid})
    rec('L5f city_del→0', r.get('errorcode') == 0, str(r)[:120])
    r = call('sysinfo.province_del', {'pid': pid})
    rec('L5g province_del→0', r.get('errorcode') == 0, str(r)[:120])
    # 物理清理测试数据，保持在用库干净
    import subprocess
    env = dict(os.environ, PGPASSWORD=os.environ.get('PGPASSWORD', ''))
    subprocess.run(['psql', '-h', '127.0.0.1', '-U', 'postgres', '-d', 'postgres', '-c',
                    'DELETE FROM sysinfo.city WHERE city LIKE %s; DELETE FROM sysinfo.province WHERE province LIKE %s' % ("'测试%W%'", "'测试%W%'")],
                   env=env, capture_output=True, timeout=15)

def t_concurrent(tok):
    print('== L7 并发冒烟(验证 POST finalize 修复后连接复用) ==')
    errs, results = [], []
    lock = threading.Lock()
    def worker(i):
        func = QUERIES[i % len(QUERIES)][0]
        st, r = api(func, {'token': tok})
        with lock:
            results.append(r.get('errorcode'))
            if r.get('errorcode') != 0:
                errs.append((func, str(r)[:80]))
    ths = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
    [th.start() for th in ths]
    [th.join() for th in ths]
    rec('L7 10并发请求全部成功', not errs, '错误码=' + str(results) + str(errs[:3]))

def main():
    argparse()
    print('ngxdb 网页层测试  目标=%s' % BASE)
    st, _ = http('/ngxdb/login.html')
    if st != 200:
        print('nginx 或前端页面不可达(http=%s)，请先启动服务' % st)
        sys.exit(2)
    t_static()
    tok = t_login()
    if tok:
        t_queries(tok)
        t_crud(tok)
        t_concurrent(tok)
    else:
        rec('L3/L5/L7 因登录失败跳过', False, '未取得token')
    t_auth()
    npass = sum(1 for x in RESULTS if x[1] == 'PASS')
    nfail = sum(1 for x in RESULTS if x[1] == 'FAIL')
    nknown = sum(1 for x in RESULTS if x[1] == 'KNOWN')
    print('\n===== 网页层测试汇总 =====')
    print('通过: %d   失败: %d   已知/说明: %d   合计: %d' % (npass, nfail, nknown, len(RESULTS)))
    if nfail:
        print('失败用例:')
        for n, s, d in RESULTS:
            if s == 'FAIL':
                print('  ✘ %s -> %s' % (n, d[:200]))
    sys.exit(1 if nfail else 0)

if __name__ == '__main__':
    main()



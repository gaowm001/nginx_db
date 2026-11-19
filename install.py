#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""NGXDB 一键安装脚本 (Windows)。
镜像 Linux 版 install.sh：源码来自本目录或其子目录(如 nginx_db_src/)，
下载与安装的产出一律放在本脚本所在目录 (d:\\ngxdb) 下：
  downloads\\  pgsql\\  nginx\\
流程: [0] 检查 -> [2] 下载 PG -> [3] 装 PG -> [4] 建库/导入 SQL
      -> [5] 下载 nginx -> [6] conf+html -> [7] admin -> [8] nginx+验证
用法: py -3 install.py  [--gui | --yes | --skip-pg | --skip-nginx |
                        --skip-db | --skip-html | --skip-admin | --skip-verify]
      py -3 install.py --uninstall        # 卸载（GUI 勾选要卸载的内容）
      py -3 install.py --cli --uninstall  # 终端逐项确认卸载
      py -3 install.py --cli --uninstall-all [--yes]   # 全部卸载
环境变量: NGINX_VERSION NGINX_PREFIX LISTEN_PORT PG_MAJOR PG_HOST PG_PORT
          PG_DBNAME PG_SUPERUSER PG_APP_USER PG_APP_PASSWORD BUILD_DIR
注意: 自研 postgres 模块需 MSVC/MinGW 编译，脚本第 [5] 步会检测并自动编译。
"""
import os, re, shutil, socket, subprocess, sys, threading, time
import urllib.request, urllib.parse, zipfile
from pathlib import Path
from datetime import datetime

# 重定向到文件时 Python 默认用系统 ANSI 编码(GBK)，强制统一为 UTF-8
for _s in (sys.stdout, sys.stderr):
    try: _s.reconfigure(encoding="utf-8", errors="replace")
    except Exception: pass

SCRIPT_DIR   = Path(__file__).resolve().parent          # 下载/安装的输出根目录（脚本所在目录）

# ---- 定位项目源码根目录（含 extension/ 与 html/） ----
_REQUIRED = ("extension/ngxdb_postgresql.sql", "html", "postgres/config", "admin/app.py")
_NAME_RE  = re.compile(r"^[^\\/]+-[^\\/]+-[0-9a-f]{7,}$")   # GitHub zipball 解压目录: 用户-仓库-commit

def _looks_like_project(d):
    return all((d / f).exists() for f in _REQUIRED) if d else False

def _find_project_dir(base):
    """在 base 附近寻找项目源码根目录；返回 Path 或 None"""
    # 0) base 自身
    if _looks_like_project(base):
        return base
    if not base.exists():
        return None
    # 1) 直接子目录（如 nginx_db_src/）
    for sub in sorted(p for p in base.iterdir() if p.is_dir()):
        if _looks_like_project(sub):
            return sub
    # 2) GitHub zipball 风格：*/*-*-commit
    for sub in sorted(p for p in base.iterdir() if p.is_dir() and _NAME_RE.match(p.name)):
        gs = [g for g in sub.iterdir() if g.is_dir()]
        if len(gs) == 1 and _looks_like_project(gs[0]):
            return gs[0]
    return None

PROJECT_SRC  = _find_project_dir(SCRIPT_DIR)
SQL_FILE     = (PROJECT_SRC / "extension" / "ngxdb_postgresql.sql") if PROJECT_SRC else (SCRIPT_DIR / "extension" / "ngxdb_postgresql.sql")
HTML_DIR     = (PROJECT_SRC / "html") if PROJECT_SRC else (SCRIPT_DIR / "html")
POSTGRES_DIR = (PROJECT_SRC / "postgres") if PROJECT_SRC else (SCRIPT_DIR / "postgres")
ADMIN_DIR    = (PROJECT_SRC / "admin") if PROJECT_SRC else (SCRIPT_DIR / "admin")
DOWNLOAD_DIR = SCRIPT_DIR / "downloads"                 # 下载与安装都放在脚本所在目录
PG_INSTALL_DIR    = SCRIPT_DIR / "pgsql"
NGINX_INSTALL_DIR = SCRIPT_DIR / "nginx"

NGINX_VERSION   = os.environ.get("NGINX_VERSION", "1.30.6")
NGINX_PREFIX    = os.environ.get("NGINX_PREFIX", str(NGINX_INSTALL_DIR))
LISTEN_PORT     = os.environ.get("LISTEN_PORT", "80")
PG_MAJOR        = os.environ.get("PG_MAJOR", "18")
PG_HOST         = os.environ.get("PG_HOST", "127.0.0.1")
PG_PORT         = os.environ.get("PG_PORT", "5432")
PG_DBNAME       = os.environ.get("PG_DBNAME", "ngxdb")
PG_SUPERUSER    = os.environ.get("PG_SUPERUSER", "postgres")
PG_APP_USER     = os.environ.get("PG_APP_USER", "gm")
PG_APP_PASSWORD = os.environ.get("PG_APP_PASSWORD") or None

SKIP_PG = SKIP_NGINX = SKIP_DB = SKIP_HTML = SKIP_ADMIN = SKIP_VERIFY = False
GUI = GUI_MODE = False   # GUI 默认开启，见 _parse_args()；GUI_MODE 供 check_prereqs 判断

# 安装步骤清单（GUI计划的每一步显示为独立状态行；终端流程按序执行）
STEPS = [
    ("chk", "[0/8] 前置检查（Python / 管理员 / 源码目录）"),
    ("pg",     "[2/8] 下载 PostgreSQL 18 Windows 二进制包"),
    ("pgx",    "[3/8] 解压 PostgreSQL 到 pgsql\\ 并 initdb 初始化 data"),
    ("db",     "[4/8] 建角色 gm/conn、建库 ngxdb、导入 SQL"),
    ("ng",     "[5/8] 下载 nginx 1.30.6 Windows 官方压缩包"),
    ("conf",   "[6/8] 生成 nginx.conf 并部署 html 前端"),
    ("admin",  "[7/8] 启动本地管理台 admin（127.0.0.1:8765）"),
    ("run",    "[8/8] 启动 nginx 并验证登录页"),
]

USE_COLOR = sys.stdout.isatty()
C_R, C_G, C_Y, C_B, C_0 = ("\033[31m","\033[32m","\033[33m","\033[34m","\033[0m") if USE_COLOR else ("","","","","")

# ---- 出错日志：所有输出同步追加写入 install.log，异常另存完整堆栈，便于事后排查 ----
LOG_FILE = SCRIPT_DIR / "install.log"
_FLOCK = threading.Lock()

def _flog(level, msg):
    try:
        with _FLOCK:
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] [{level}] {msg}\n")
    except Exception:
        pass

def log(m):  print(f"{C_B}[NGXDB]{C_0} {m}"); _flog("INFO", str(m))
def ok(m):   print(f"{C_G}[  OK ]{C_0} {m}"); _flog("OK", str(m))
def warn(m): print(f"{C_Y}[ WARN] {C_0} {m}"); _flog("WARN", str(m))
def err(m):  print(f"{C_R}[ERROR]{C_0} {m}", file=sys.stderr); _flog("ERROR", str(m))

def log_exc(prefix=""):
    """把当前异常的完整调用堆栈写入日志文件（在 except 块中调用）"""
    import traceback
    if prefix:
        _flog("TRACE", prefix)
    _flog("TRACE", traceback.format_exc().rstrip())
    return traceback.format_exc()

# EDB 会对无 UA 的请求返回 403
HTTP_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
           "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")

# 下载进度回调（GUI 会替换为真正的进度条更新函数；终端模式为 None）
PROGRESS = {"cb": None}

# GUI 交互钩子：终端模式全部为 None（保持原行为不变）
UI = {
    "step": None,     # fn(key, state)   state: run|ok|skip|fail
    "progress": None, # fn(done, total, text)  下载进度
    "confirm": None,  # fn(title, msg) -> bool  关键操作前确认
    "ask": None,      # fn(title, prompt, default) -> str  询问输入
}

def check_prereqs():
    global GUI_MODE
    log("==== [0/8] 前置检查 ====")
    if sys.platform != "win32": raise SystemExit("本脚本仅支持 Windows")
    if sys.version_info < (3, 8): raise SystemExit(f"Python 版本过低: {sys.version}")
    is_admin = True
    try:
        import ctypes
        is_admin = bool(ctypes.windll.shell32.IsUserAnAdmin())
    except Exception:
        pass
    if not is_admin:
        warn("当前非管理员，写 d:\\ngxdb / 启动服务将失败（建议右键以管理员运行）")
        if not GUI_MODE:
            try:
                if input("是否继续? [y/N] ").strip().lower() not in ("y","yes"):
                    raise SystemExit("已退出")
            except EOFError:
                pass
    for p, desc in [(SQL_FILE,"SQL 文件"),(HTML_DIR,"前端目录"),
                    (POSTGRES_DIR/"config","postgres 模块 config"),
                    (ADMIN_DIR/"app.py","管理台 app.py")]:
        if not p.exists(): raise SystemExit(f"缺少 {desc}: {p}")
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    ok(f"Python {sys.version.split()[0]} · 项目目录 {PROJECT_SRC}")
    ok(f"安装目录 {SCRIPT_DIR} · 下载目录 {DOWNLOAD_DIR}")
    ok(f"运行日志 {LOG_FILE}（出错时同步记录完整堆栈）")

def _report_progress(done, total, text):
    cb = PROGRESS.get("cb") or UI.get("progress")
    if cb:
        try:
            cb(done, total, text)
        except Exception:
            pass

def _verify_zip_structure(path):
    """校验 zip 完整性：EOCD 可定位，且每个成员的 local header 签名(PK\\x03\\x04)正确。
    返回 (True, 描述) 或 (False, 原因)。能精确抓住'中央目录与数据错位'类损坏。"""
    p = Path(path)
    try:
        if not zipfile.is_zipfile(p):
            return False, "找不到 End of Central Directory（不是有效的 zip）"
        size = p.stat().st_size
        with zipfile.ZipFile(p) as z:
            infos = z.infolist()
            if not infos:
                return False, "zip 内没有任何成员"
            with open(p, "rb") as fh:
                for info in infos:
                    off = info.header_offset
                    if off < 0 or off + 30 > size:
                        return False, (f"成员 {info.filename!r} 的偏移越界 "
                                       f"({off}，文件大小 {size})")
                    fh.seek(off)
                    if fh.read(4) != b"PK\x03\x04":
                        return False, (f"成员 {info.filename!r} 的 local header "
                                       f"签名不符（文件已损坏）")
            return True, f"{len(infos)} 个成员全部校验通过"
    except Exception as e:
        return False, f"校验异常: {e}"

def download(url, dest, desc="", attempts=3):
    """Download to dest with browser UA, resume support and zip integrity check."""
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    name = desc or dest.name
    is_zip = dest.suffix.lower() == ".zip"
    tmp = dest.with_suffix(dest.suffix + ".part")
    # 已有成品：zip 必须通过结构校验，否则删除重新下载（自愈坏文件）
    if dest.exists() and dest.stat().st_size > 0:
        if is_zip:
            good, why = _verify_zip_structure(dest)
            if good:
                ok(f"{name} 已存在且校验通过，跳过下载")
                return dest
            warn(f"{name} 已存在但校验失败（{why}），删除后重新下载")
            dest.unlink(missing_ok=True)
        else:
            ok(f"{name} 已存在，跳过下载: {dest}")
            return dest
    last_err = None
    for attempt in range(1, attempts + 1):
        have = tmp.stat().st_size if tmp.exists() else 0
        if attempt > 1 or have:
            log(f"下载 {name}: 第 {attempt}/{attempts} 次尝试"
                + (f"（续传 {have//1024//1024} MB）" if have else ""))
        else:
            log(f"下载 {name}: {url}")
        try:
            headers = {"User-Agent": HTTP_UA, "Accept": "*/*"}
            if have:
                headers["Range"] = f"bytes={have}-"
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=120) as resp:
                status = getattr(resp, "status", 200) or 200
                if status != 206:
                    have = 0
                total = 0
                if resp.headers.get("Content-Length"):
                    try:
                        total = int(resp.headers["Content-Length"]) + have
                    except ValueError:
                        total = 0
                _report_progress(have, total, name)
                mode = "ab" if have else "wb"
                with open(tmp, mode) as fh:
                    done = have
                    last = 0.0
                    while True:
                        chunk = resp.read(1024 * 256)
                        if not chunk:
                            break
                        fh.write(chunk)
                        done += len(chunk)
                        now = time.time()
                        if now - last >= 0.3:
                            last = now
                            _report_progress(done, total, name)
            size = tmp.stat().st_size
            if total and size < total:
                raise RuntimeError(
                    f"下载不完整: {size}/{total} 字节（服务器提前断开）")
            if is_zip:                      # 下载完成即校验，坏包立即重下
                good, why = _verify_zip_structure(tmp)
                if not good:
                    raise RuntimeError(f"zip 结构校验失败: {why}")
                ok(f"zip 校验通过: {why}")
            tmp.replace(dest)
            ok(f"已下载 {dest.name} ({size//1024//1024} MB)")
            return dest
        except Exception as e:
            last_err = e
            warn(f"下载出错（第 {attempt} 次）: {e}")
            tmp.unlink(missing_ok=True)     # 删除残留，下次从头下载
            time.sleep(2)
    raise RuntimeError(f"下载失败（已重试 {attempts} 次）: {last_err}")

# ===================== [2/8] 下载 PostgreSQL 安装器 =====================
# 使用 EDB 官方 Windows 二进制 zip（免安装、可直接解压到脚本目录）：
#   解压后结构为 pgsql\bin  pgsql\lib  pgsql\share  pgsql\doc
#   data 目录由 initdb 现场初始化，因此无需管理员 msi / 无需注册服务。
# EDB 对无 User-Agent 的请求返回 403，download() 已带浏览器 UA。
PG_BINARIES_VER = os.environ.get("PG_BINARIES_VER", f"{PG_MAJOR}.4")

def pg_installer_candidates():
    """返回 (url, filename, kind) 列表；kind: 'zip'(便携二进制) | 'edb'(安装器)"""
    return [
        (f"https://get.enterprisedb.com/postgresql/postgresql-{PG_BINARIES_VER}-1-windows-x64-binaries.zip",
         f"postgresql-{PG_BINARIES_VER}-1-windows-x64-binaries.zip", "zip"),
        (f"https://get.enterprisedb.com/postgresql/postgresql-17.6-1-windows-x64-binaries.zip",
         "postgresql-17.6-1-windows-x64-binaries.zip", "zip"),
    ]

def step_download_pg():
    log(f"==== [2/8] 下载 PostgreSQL {PG_MAJOR} Windows 安装器 ====")
    if SKIP_PG: log("跳过"); return None, None
    last = None
    for url, name, kind in pg_installer_candidates():
        try:
            dest = download(url, DOWNLOAD_DIR/name, f"PostgreSQL {PG_MAJOR} ({kind})")
            ok(f"使用 {name} ({kind})"); return dest, kind
        except Exception as e:
            warn(f"下载失败 {url}: {e}"); last = e
    raise RuntimeError(f"所有 PostgreSQL 下载源都失败: {last}")

# ===================== [3/8] 安装 PostgreSQL =====================
def pg_bin_dir():
    d = PG_INSTALL_DIR/"bin"
    if (d/"psql.exe").exists(): return d
    if PG_INSTALL_DIR.exists():
        for sub in PG_INSTALL_DIR.iterdir():
            if sub.is_dir() and (sub/"bin"/"psql.exe").exists(): return sub/"bin"
    return None

def _check_disk_space(target, need_gb=2.0):
    """解压前确认磁盘剩余空间，避免解压到一半失败"""
    try:
        free = shutil.disk_usage(str(target)).free
        if free < need_gb * 1024**3:
            raise RuntimeError(
                f"磁盘剩余空间不足: {free/1024**3:.1f} GB < 需要 {need_gb} GB"
                f"（目标 {target}）")
    except RuntimeError:
        raise
    except Exception:
        pass  # 查询失败不阻塞安装

def _extract_zip_progress(zip_path, target, label="PostgreSQL"):
    """解压 zip，带每文件进度与完整性校验；损坏时给出可执行的修复指引"""
    zip_path = Path(zip_path)
    log(f"校验压缩包: {zip_path.name}")
    if not zipfile.is_zipfile(zip_path):
        zip_path.unlink(missing_ok=True)
        raise RuntimeError(
            f"{zip_path.name} 不是有效的 zip 文件（可能下载不完整或被替换为错误页面），"
            f"已删除该文件，请重新运行安装（会自动重新下载）")
    try:
        with zipfile.ZipFile(zip_path) as z:
            names = z.namelist()
            if not names:
                raise RuntimeError(f"{zip_path.name} 内没有任何文件")
            log(f"解压 {len(names)} 个文件 -> {target}")
            _check_disk_space(target, need_gb=2.0)
            bad = z.testzip()
            if bad is not None:
                raise RuntimeError(f"压缩包内文件损坏: {bad}")
            done = 0
            for info in z.infolist():
                z.extract(info, str(target))
                done += 1
                if done % 200 == 0 or done == len(names):
                    _report_progress(done, len(names),
                                     f"解压 {label}: {done}/{len(names)} 个文件")
    except zipfile.BadZipFile as e:
        zip_path.unlink(missing_ok=True)
        raise RuntimeError(
            f"{zip_path.name} 解压失败（{e}）：文件可能下载不完整。"
            f"已删除该文件，请重新运行安装（会自动重新下载）")
    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(f"解压失败: {e}")

def _force_rmtree(path):
    """Windows 下强制递归删除（对只读属性文件先 chmod 再删，避免残留）"""
    import stat as _stat
    def onerror(func, p, exc_info):
        try:
            os.chmod(p, _stat.S_IWRITE)
            func(p)
        except Exception:
            pass
    shutil.rmtree(path, onerror=onerror)

def _flatten_pg_dir():
    """EDB zip 解压后常带顶层 pgsql/ 目录；拍平到 PG_INSTALL_DIR 根。
    Windows 的 rename 在目标已存在时报 WinError 5，故先强制删除同名残留；
    若个别条目移动失败，则退而把安装根目录指向嵌套目录本身。"""
    global PG_INSTALL_DIR
    nested = PG_INSTALL_DIR/"pgsql"
    if not (nested/"bin"/"psql.exe").exists():
        return
    log(f"整理目录结构: {nested} -> {PG_INSTALL_DIR}")
    failed = []
    for child in list(nested.iterdir()):
        tgt = PG_INSTALL_DIR/child.name
        try:
            if tgt.exists():
                if tgt.is_dir():
                    _force_rmtree(tgt)
                else:
                    tgt.unlink()
            shutil.move(str(child), str(tgt))
        except Exception as e:
            warn(f"移动 {child.name!r} 失败: {e}")
            failed.append(child.name)
    if failed:
        warn(f"拍平未完成（{len(failed)} 项），改用 {nested} 作为 PostgreSQL 根目录")
        PG_INSTALL_DIR = nested
    else:
        try:
            nested.rmdir()
        except OSError:
            pass

def step_install_pg(installer, kind):
    log(f"==== [3/8] 安装 PostgreSQL {PG_MAJOR} ====")
    if SKIP_PG: log("跳过"); return
    if pg_bin_dir():
        bd = pg_bin_dir()
        _ensure_python313_for_plpython()
        if (bd.parent/"data"/"PG_VERSION").exists():
            ok(f"PostgreSQL 已安装且 data 已初始化: {bd.parent}")
            return
        ok(f"PostgreSQL 二进制已存在: {bd.parent}，补做 data 初始化")
        _init_pg_data_dir()
        _ensure_pg_running()
        return
    if not installer: raise SystemExit("缺少 PostgreSQL 安装器")
    PG_INSTALL_DIR.mkdir(parents=True, exist_ok=True)
    if kind == "zip":
        _extract_zip_progress(installer, PG_INSTALL_DIR)
        _flatten_pg_dir()
        if not pg_bin_dir():
            raise SystemExit(
                f"解压完成但找不到 psql.exe（检查 {PG_INSTALL_DIR} 目录结构）")
        ok(f"PostgreSQL 二进制已解压到 {PG_INSTALL_DIR}")
        _ensure_python313_for_plpython()
        _init_pg_data_dir()
        _ensure_pg_running()
    else:
        args = [str(installer),"/SILENT","/NO_SHORTCUTS","/REBOOT=NONE",
                f"/DIR={PG_INSTALL_DIR}",
                "/COMPONENTS=client,server,plperl,plpython,plruby,pgcrypto,adminpack,uuid-ossp",
                "/FEATURES=Complete","/INCLUDEDRIVERS=false","/UPDATEPATH=false"]
        log("执行 EDB 静默安装...")
        r = subprocess.run(args, capture_output=True, text=True, timeout=600)
        if r.returncode != 0:
            err((r.stdout+r.stderr).strip()); raise RuntimeError(f"EDB 退出码 {r.returncode}")
        ok(f"PostgreSQL 已安装到 {PG_INSTALL_DIR}")
        _ensure_python313_for_plpython()
        _ensure_pg_running()

# EDB 的 PG18 Windows 二进制中 plpython3.dll 链接的是 python313.dll，
# 但 binaries zip 未捆绑 Python 运行时。下载官方 embeddable 包补齐 DLL，
# 并通过 PYTHONHOME 让 plpython 能定位标准库（SQL 中 gm.http_get/post 需要）。
PYTHON313_VER = os.environ.get("PYTHON313_VER", "3.13.3")

def _ensure_python313_for_plpython():
    lib = pg_bin_dir().parent/"lib"
    if (lib/"python313.dll").exists() and (pg_bin_dir()/"python313.dll").exists():
        ok("python313.dll 已就位"); return
    log("下载 Python 3.13 embeddable（plpython3u 运行时依赖）...")
    url = (f"https://www.python.org/ftp/python/{PYTHON313_VER}/"
           f"python-{PYTHON313_VER}-embed-amd64.zip")
    z = download(url, DOWNLOAD_DIR/f"python-{PYTHON313_VER}-embed-amd64.zip",
                 "Python 3.13 embeddable")
    home = PG_INSTALL_DIR/"python313"
    home.mkdir(parents=True, exist_ok=True)
    _extract_zip_progress(z, home)
    # 拍平 embeddable 顶层目录（如有）
    nested = home/Path(z).stem
    if (nested/"python313.dll").exists():
        for c in list(nested.iterdir()):
            shutil.move(str(c), str(home/c.name))
        try: nested.rmdir()
        except OSError: pass
    # PG 用 LOAD_LIBRARY_SEARCH_DEFAULT_DIRS 加载扩展：其依赖搜索范围是
    # 应用程序目录（bin）而非扩展 DLL 所在的 lib，因此两处都放以确保可加载。
    copied = []
    for name in ("python313.dll","vcruntime140.dll","vcruntime140_1.dll",
                 "libcrypto-3-x64.dll","libssl-3-x64.dll"):
        src = home/name
        if src.exists():
            shutil.copy2(src, lib/name)
            shutil.copy2(src, pg_bin_dir()/name)
            copied.append(name)
    if "python313.dll" in copied:
        ok(f"plpython3u 运行时已补齐: {', '.join(copied)} -> {lib}")
    else:
        warn(f"未能在 embeddable 包中找到 python313.dll，plpython3u 可能不可用")

def _init_pg_data_dir():
    bd = pg_bin_dir()
    if not bd: raise SystemExit("zip 解压后找不到 psql.exe")
    data_dir = bd.parent/"data"
    if data_dir.exists() and (data_dir/"PG_VERSION").exists():
        ok(f"data 目录已初始化: {data_dir}"); return
    data_dir.mkdir(parents=True, exist_ok=True)
    log(f"initdb 初始化 {data_dir} ...")
    r = subprocess.run([str(bd/"initdb.exe"),"-D",str(data_dir),
                        "-U",PG_SUPERUSER,"-A","trust","--locale=C"],
                       capture_output=True, text=True, timeout=300)
    if r.returncode != 0:
        err((r.stdout+r.stderr).strip()); raise RuntimeError("initdb 失败")
    ok(f"data 目录已初始化: {data_dir}")

def _ensure_pg_running():
    bd = pg_bin_dir()
    if not bd: return
    data_dir = bd.parent/"data"
    try:
        r = subprocess.run([str(bd/"pg_ctl.exe"),"status","-D",str(data_dir)],
                           capture_output=True, text=True, timeout=10)
        if "not running" not in (r.stdout+r.stderr) and r.returncode == 0:
            ok("PostgreSQL 服务已在运行"); return
    except Exception: pass
    log(f"启动 PostgreSQL (data={data_dir}, port={PG_PORT}) ...")
    # Windows 下 subprocess 不经 shell，单引号会原样传给 postgres，
    # 导致 listen_addresses 变成 "'127.0.0.1'" 而无法解析主机名。
    # 另外 pg_ctl 会 spawn postgres 作为后台进程并继承 stdout/stderr 管道
    # 句柄；若 capture_output 则读端永远等不到 EOF 而卡死，改为写入日志文件。
    # plpython3.dll 依赖系统 Python 的 python3xx.dll（EDB 包未捆绑），
    # 启动前把当前 Python 解释器目录前置到 PATH 以便 DLL 搜索。
    env = os.environ.copy()
    pydir = str(Path(sys.executable).parent)
    if pydir and pydir not in env.get("PATH", ""):
        env["PATH"] = pydir + os.pathsep + env.get("PATH", "")
    # plpython3u 需要：DLL 已补到 lib/，PYTHONHOME 指向 embeddable 标准库
    if (bd.parent/"lib"/"python313.dll").exists():
        env["PYTHONHOME"] = str(bd.parent/"python313")
    try:
        with open(bd.parent/"pg_ctl.log", "ab") as lf:
            r = subprocess.run([str(bd/"pg_ctl.exe"),"start","-D",str(data_dir),
                                "-l",str(bd.parent/"pg.log"),
                                "-o",f"-p {PG_PORT} -c listen_addresses={PG_HOST}"],
                               stdout=lf, stderr=subprocess.STDOUT, timeout=120,
                               env=env)
    except subprocess.TimeoutExpired:
        r = None
    if r is None or r.returncode != 0:
        warn(f"pg_ctl start 失败或超时（可能服务已注册），详见 {bd.parent/'pg_ctl.log'}")
    else: ok("PostgreSQL 已启动")
    time.sleep(1)

# ===================== [4/8] 建库/建角色/导入 SQL =====================
def psql(bin_dir, *args, password=None, as_user=None, db="postgres", timeout=300):
    cmd = [str(bin_dir/"psql.exe"), "-h", PG_HOST, "-p", str(PG_PORT),
           "-U", as_user or PG_SUPERUSER, "-d", db,
           "-v", "ON_ERROR_STOP=1", "-tA", *args]
    env = os.environ.copy()
    if password: env["PGPASSWORD"] = password
    log(f"psql {' '.join(a for a in args if not a.startswith('-')) or '(empty)'}")
    r = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=timeout)
    if r.stdout.strip():
        for ln in r.stdout.splitlines()[-8:]: print(f"    {ln}")
    if r.returncode != 0:
        err(r.stderr.strip() or "(无 stderr)")
        raise RuntimeError(f"psql 失败: {' '.join(map(str, args))}: "
                           f"{(r.stderr or '').strip()[:200]}")
    return r.stdout

def step_init_db(pg_password):
    log(f"==== [4/8] 初始化数据库角色与 SQL 导入 ====")
    if SKIP_DB: log("跳过"); return
    bd = pg_bin_dir()
    if not bd: raise SystemExit("找不到 PostgreSQL bin 目录")
    _ensure_pg_running()
    for role in [PG_APP_USER, "conn"]:
        try:
            psql(bd, "-c", f"CREATE ROLE {role} LOGIN PASSWORD '{pg_password}';",
                 password=pg_password, as_user=PG_SUPERUSER)
            ok(f"角色 {role} 已创建")
        except RuntimeError as e:
            if "already exists" in str(e):
                ok(f"角色 {role} 已存在，跳过")
            else:
                raise
    out = psql(bd, "-tA", "-c", f"SELECT 1 FROM pg_database WHERE datname='{PG_DBNAME}';",
               password=pg_password, as_user=PG_SUPERUSER)
    if out.strip() == "1":
        warn(f"数据库 {PG_DBNAME} 已存在，删除后重建以保证 SQL 可重复导入")
        # 先断开现有连接（nginx 模块连接池可能占用），再强制删除
        psql(bd, "-c", f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                       f"WHERE datname='{PG_DBNAME}' AND pid <> pg_backend_pid();",
             password=pg_password, as_user=PG_SUPERUSER, db="postgres")
        psql(bd, "-c", f"DROP DATABASE {PG_DBNAME} WITH (FORCE);",
             password=pg_password, as_user=PG_SUPERUSER)
    psql(bd, "-c", f"CREATE DATABASE {PG_DBNAME} OWNER {PG_APP_USER};",
         password=pg_password, as_user=PG_SUPERUSER)
    ok(f"数据库 {PG_DBNAME} 已创建")
    tmp = DOWNLOAD_DIR / "ngxdb_import.sql"
    tmp.write_text(SQL_FILE.read_text(encoding="utf-8"), encoding="utf-8")
    psql(bd, "-f", str(tmp), password=pg_password, as_user=PG_SUPERUSER, db=PG_DBNAME)
    ok(f"SQL 已导入到 {PG_DBNAME}")
    try:
        psql(bd, "-c",
             "CREATE TABLE IF NOT EXISTS public.serverlog ("
             "logid bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, "
             "clientip inet, pckname varchar(255), funcname varchar(255), "
             "content text, logtime timestamp without time zone, serverip inet, "
             "params text, operatorid bigint, res text, head text);",
             password=pg_password, as_user=PG_SUPERUSER, db=PG_DBNAME)
    except RuntimeError:
        warn("serverlog 表可能已存在（忽略）")
    for ext in ("pgcrypto", "plpython3u"):
        try:
            psql(bd, "-c", f"CREATE EXTENSION IF NOT EXISTS {ext};",
                 password=pg_password, as_user=PG_SUPERUSER, db=PG_DBNAME)
            ok(f"扩展 {ext} 已启用")
        except RuntimeError as e:
            warn(f"扩展 {ext} 启用失败（可能组件未安装）: {str(e)[:120]}")

# ===================== [5/8] 下载 nginx 官方二进制 =====================
def _stop_nginx():
    """停止正在运行的 nginx，避免 nginx.exe 被占用导致解压/覆盖失败"""
    exe = Path(NGINX_PREFIX)/"nginx.exe"
    if exe.exists():
        try:
            subprocess.run([str(exe), "-s", "stop"], cwd=str(NGINX_PREFIX),
                           capture_output=True, timeout=15)
        except Exception:
            pass
    try:
        r = subprocess.run(["taskkill", "/F", "/IM", "nginx.exe", "/T"],
                           capture_output=True, text=True, timeout=15)
        if r.returncode == 0:
            log("已停止运行中的 nginx 进程")
    except Exception:
        pass

def _flatten_nginx_dir(target):
    """nginx zip 自带顶层 nginx-<ver>/ 目录；拍平到安装根目录"""
    nested = None
    for sub in target.iterdir():
        if sub.is_dir() and re.match(rf"nginx-{NGINX_VERSION}$", sub.name):
            nested = sub; break
    if nested is None:
        return
    log(f"整理目录结构: {nested} -> {target}")
    for child in nested.iterdir():
        tgt = target/child.name
        if tgt.exists():
            if tgt.is_dir(): _force_rmtree(tgt)
            else: tgt.unlink(missing_ok=True)
        shutil.move(str(child), str(tgt))
    nested.rmdir()

def step_download_nginx():
    log(f"==== [5/8] 下载 nginx {NGINX_VERSION} (Windows 官方二进制) ====")
    if SKIP_NGINX: log("跳过"); return
    url = f"https://nginx.org/download/nginx-{NGINX_VERSION}.zip"
    dest = download(url, DOWNLOAD_DIR/f"nginx-{NGINX_VERSION}.zip", "nginx zip")
    target = Path(NGINX_PREFIX)
    target.mkdir(parents=True, exist_ok=True)
    _stop_nginx()
    log(f"解压到 {target}")
    _extract_zip_progress(dest, target, label="nginx")
    _flatten_nginx_dir(target)
    if not (target/"nginx.exe").exists():
        raise SystemExit(f"解压完成但找不到 nginx.exe（检查 {target} 目录结构）")
    ok(f"nginx 二进制已部署到 {target}")
    exe = target/"nginx.exe"
    global NGINX_HAS_PG_MODULE
    log("检查自研 postgres 模块...")
    try:
        r = subprocess.run([str(exe), "-V"], capture_output=True, text=True)
        NGINX_HAS_PG_MODULE = "postgres" in (r.stdout+r.stderr)
        if NGINX_HAS_PG_MODULE:
            ok("检测到自研 postgres 模块，/func 网关可用")
        else:
            warn("当前 nginx.exe 为官方 Windows 二进制，不含自研 postgres 模块。")
            step_build_nginx_module()
    except Exception as e:
        warn(f"nginx -V 执行失败（忽略）: {e}")

# ============ [5b] 用 MinGW 编译含自研 postgres 模块的 nginx ============
MMAN_SHIM = r"""#ifndef NGXDB_MMAN_SHIM_H
#define NGXDB_MMAN_SHIM_H
/* Windows(MinGW) 下的 <sys/mman.h> 最小垫片，仅供 nginx 自研 postgres 模块。
 * nginx 在 Windows 上是单进程多线程模型，模块里用于共享"重载代数"的
 * mmap(MAP_SHARED|MAP_ANONYMOUS) 用进程内虚拟内存即可等价实现。 */
#include <stddef.h>
#include <windows.h>
#define PROT_NONE  0x0
#define PROT_READ  0x1
#define PROT_WRITE 0x2
#define PROT_EXEC  0x4
#define MAP_SHARED    0x0001
#define MAP_PRIVATE   0x0002
#define MAP_ANONYMOUS 0x0020
#define MAP_ANON      MAP_ANONYMOUS
#define MAP_FIXED     0x0010
#define MAP_FAILED    ((void *) -1)
static void * __attribute__((unused)) mmap(void *addr, size_t length, int prot,
                  int flags, int fd, long long offset)
{
    (void)addr; (void)prot; (void)flags; (void)fd; (void)offset;
    return VirtualAlloc(NULL, length ? length : 4096,
                        MEM_RESERVE | MEM_COMMIT, PAGE_READWRITE);
}
static int __attribute__((unused)) munmap(void *addr, size_t length)
{
    (void)length;
    return VirtualFree(addr, 0, MEM_RELEASE) ? 0 : -1;
}
#endif /* NGXDB_MMAN_SHIM_H */
"""

BUILD_SH = r"""#!/bin/sh
set -e
export PATH="__MSYSUSR__:__MINGW__/bin:$PATH"
cd __NGINXSRC__
if [ ! -f objs/Makefile ]; then
  ./configure \
    --crossbuild=win32 \
    --with-cc=__MINGW__/bin/gcc.exe \
    --add-module=__PROJ__/postgres \
    --with-pcre=__BUILD__/pcre2-10.44 \
    --without-http_gzip_module \
    --with-cc-opt='-I__PG__/include -I__BUILD__/shim' \
    --with-ld-opt='-L__BUILD__/minwlib -L__PG__/lib' \
    --prefix=__NGINX__
fi
echo CONFIGURE_OK
make -f objs/Makefile -j8
echo MAKE_OK
"""

def _mpath(p):
    """Windows 路径 -> MSYS 路径（D:/x/y -> /d/x/y）"""
    s = str(p).replace("\\", "/")
    if len(s) > 1 and s[1] == ":":
        s = "/" + s[0].lower() + s[2:]
    return s

def _mingw_dir():
    for cand in (SCRIPT_DIR/"mingw64",):
        if (cand/"bin/gcc.exe").exists():
            return cand
    return None

def _run_bash(bash, script_path):
    """运行 bash 脚本，实时把输出写进安装日志；返回退出码"""
    proc = subprocess.Popen([str(bash), _mpath(script_path)],
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    for raw in iter(proc.stdout.readline, b""):
        line = raw.decode("utf-8", "replace").rstrip()
        if line:
            log(f"    {line}")
    proc.wait()
    return proc.returncode

def step_build_nginx_module():
    """官方 nginx.exe 不含自研 postgres 模块时，用 MinGW + MSYS2 现场编译。
    任一环节不可用则保留降级 conf（/func 返回 503），不阻塞安装。"""
    global NGINX_HAS_PG_MODULE
    if NGINX_HAS_PG_MODULE or SKIP_NGINX:
        return
    log("==== [5b] 编译含自研 postgres 模块的 nginx ====")
    mingw = _mingw_dir()
    if not mingw:
        warn("未找到 MinGW gcc，跳过模块编译（/func 将返回 503）")
        return
    try:
        build = SCRIPT_DIR/"build"
        build.mkdir(parents=True, exist_ok=True)
        bash = build/"msys64/usr/bin/bash.exe"
        if not bash.exists():
            log("下载 MSYS2 基础包（约 44MB）...")
            sfx = download("https://github.com/msys2/msys2-installer/releases/"
                           "latest/download/msys2-base-x86_64-latest.sfx.exe",
                           build/"msys2.sfx.exe", "msys2 base sfx")
            log("解压 MSYS2 ...")
            subprocess.run([str(sfx), "-y", f"-o{build}"], check=True, timeout=600)
        if not bash.exists():
            warn("MSYS2 不可用，跳过模块编译"); return
        r = subprocess.run([str(bash), "-c",
                            "export PATH=/usr/bin:$PATH; command -v make || "
                            "pacman -Sy --noconfirm make"],
                           capture_output=True, text=True, timeout=900)
        if not (build/"msys64/usr/bin/make.exe").exists():
            warn(f"make 安装失败，跳过模块编译: {(r.stderr or r.stdout)[-200:]}")
            return
        ok("MSYS2 make 就绪")
        srcgz = download(f"https://nginx.org/download/nginx-{NGINX_VERSION}.tar.gz",
                         DOWNLOAD_DIR/f"nginx-{NGINX_VERSION}.tar.gz", "nginx 源码")
        nginx_src = build/f"nginx-{NGINX_VERSION}"
        if not (nginx_src/"configure").exists():
            log("解压 nginx 源码 ...")
            subprocess.run(["tar", "-xzf", str(srcgz), "-C", str(build)],
                           check=True, timeout=300)
        pcre_zip = download("https://github.com/PCRE2Project/pcre2/releases/"
                            "download/pcre2-10.44/pcre2-10.44.zip",
                            DOWNLOAD_DIR/"pcre2-10.44.zip", "pcre2 源码")
        if not (build/"pcre2-10.44/src").exists():
            log("解压 pcre2 ...")
            subprocess.run(["tar", "-xf", str(pcre_zip), "-C", str(build)],
                           check=True, timeout=300)
        shim = build/"shim/sys"
        shim.mkdir(parents=True, exist_ok=True)
        (shim/"mman.h").write_text(MMAN_SHIM, encoding="utf-8")
        minwlib = build/"minwlib"
        minwlib.mkdir(parents=True, exist_ok=True)
        pgbin = pg_bin_dir() or (Path(os.environ.get("PG_INSTALL_DIR", "pgsql"))/"bin")
        if not (minwlib/"libpq.a").exists():
            log("生成 libpq 导入库 ...")
            subprocess.run([str(mingw/"bin/gendef.exe"), "libpq.def",
                            str(pgbin/"libpq.dll")], cwd=str(build),
                           capture_output=True, timeout=120)
            subprocess.run([str(mingw/"bin/dlltool.exe"), "-d", "libpq.def",
                            "-D", "libpq.dll", "-l", str(minwlib/"libpq.a")],
                           cwd=str(build), capture_output=True, timeout=120)
        bsh = build/"build_nginx.sh"
        sh = (BUILD_SH.replace("__MSYSUSR__", _mpath(build/"msys64/usr/bin"))
                      .replace("__MINGW__", _mpath(mingw))
                      .replace("__NGINXSRC__", _mpath(nginx_src))
                      .replace("__PROJ__", _mpath(PROJECT_SRC))
                      .replace("__BUILD__", _mpath(build))
                      .replace("__PG__", _mpath(pgbin.parent))
                      .replace("__NGINX__", _mpath(Path(NGINX_PREFIX))))
        bsh.write_text(sh, encoding="utf-8", newline="\n")
        log("开始编译 nginx + postgres 模块（约 3-5 分钟）...")
        rc = _run_bash(bash, bsh)
        newexe = nginx_src/"objs/nginx.exe"
        if rc != 0 or not newexe.exists():
            warn(f"模块编译未成功（rc={rc}），保留官方 nginx.exe 与降级 conf")
            return
        target = Path(NGINX_PREFIX)
        _stop_nginx()
        official = target/"nginx.official.exe.bak"
        if not official.exists():
            (target/"nginx.exe").replace(official)
        shutil.copy2(newexe, target/"nginx.exe")
        for dll in ("libpq.dll", "libcrypto-3-x64.dll", "libssl-3-x64.dll",
                    "libintl-9.dll", "libiconv-2.dll", "liblz4.dll", "zlib1.dll",
                    "libwinpthread-1.dll", "libgcc_s_seh-1.dll"):
            for src in (newexe.parent, pgbin, mingw/"bin"):
                if (src/dll).exists():
                    shutil.copy2(src/dll, target/dll)
                    break
        NGINX_HAS_PG_MODULE = True
        ok("自研 postgres 模块编译完成，nginx.exe 已替换（原版备份为 nginx.official.exe.bak）")
    except Exception as e:
        log_exc()
        warn(f"模块编译环节失败（不影响其余安装）: {e}")


# ===================== [6/8] 生成 nginx.conf + 部署 html =====================
NGINX_HAS_PG_MODULE = False

FUNC_BLOCK_MODULE = """\
        # /func/ 由自研 postgres 模块直接查询数据库并返回 JSON
        location /func/ {
            rewrite ^/func/(.*)$ /$1 break;
            postgresconn "host=__PG_HOST__ port=__PG_PORT__ dbname=__PG_DBNAME__ user=__PG_APP_USER__ password='__PG_APP_PASSWORD__'";
        }"""

FUNC_BLOCK_FALLBACK = """\
        # 官方 nginx.exe 未编译自研 postgres 模块，/func 网关暂不可用。
        # 编译含模块的 nginx.exe 后，可参考项目模板恢复 postgresconn 配置。
        # 注意：message 键与前端 core.js 的错误提示约定一致。
        location /func/ {
            default_type application/json;
            return 503 '{"errorcode":-1,"message":"postgres 模块未编译进此 nginx.exe，请先完成模块编译"}';
        }"""

NGINX_CONF_TEMPLATE = """worker_processes  auto;

events {
    worker_connections  4096;
}

http {
    include       mime.types;
    default_type  application/octet-stream;
    sendfile      on;
    keepalive_timeout  65;

    server {
        listen       __LISTEN_PORT__;
        server_name  _;

        root   html;
        index  index.html index.htm;

        location / {
            try_files  $uri $uri/ =404;
        }

__FUNC_BLOCK__
    }
}
"""

def _write_nginx_conf(use_module=None):
    """生成 nginx.conf；use_module=None 时按检测结果自动选择 /func 配置块"""
    target = Path(NGINX_PREFIX)
    conf = target/"conf"/"nginx.conf"
    if use_module is None:
        use_module = NGINX_HAS_PG_MODULE
    func_block = FUNC_BLOCK_MODULE if use_module else FUNC_BLOCK_FALLBACK
    content = (NGINX_CONF_TEMPLATE
               .replace("__FUNC_BLOCK__", func_block)
               .replace("__LISTEN_PORT__", LISTEN_PORT)
               .replace("__PG_HOST__", PG_HOST)
               .replace("__PG_PORT__", str(PG_PORT))
               .replace("__PG_DBNAME__", PG_DBNAME)
               .replace("__PG_APP_USER__", PG_APP_USER)
               .replace("__PG_APP_PASSWORD__", PG_APP_PASSWORD or "CHANGE_ME"))
    conf.write_text(content, encoding="utf-8")
    return conf, use_module

def step_conf_html():
    log("==== [6/8] 生成 nginx.conf + 部署前端 ====")
    if SKIP_HTML: log("跳过"); return
    target = Path(NGINX_PREFIX)
    (target/"conf").mkdir(parents=True, exist_ok=True)
    conf = target/"conf"/"nginx.conf"
    if conf.exists():
        bak = conf.with_suffix(f".conf.bak.{datetime.now():%Y%m%d%H%M%S}")
        conf.rename(bak)
        ok(f"旧 nginx.conf 已备份: {bak.name}")
    conf, with_module = _write_nginx_conf()
    if with_module:
        ok(f"nginx.conf 已生成（含 postgres 网关）: {conf}")
    else:
        warn(f"nginx.conf 已生成（降级版，/func 返回 503）: {conf}")
    html_target = target/"html"
    html_target.mkdir(parents=True, exist_ok=True)
    for item in HTML_DIR.iterdir():
        dest = html_target/item.name
        if item.is_dir():
            if dest.exists(): shutil.rmtree(dest, ignore_errors=True)
            shutil.copytree(item, dest, dirs_exist_ok=True)
        else:
            shutil.copy2(item, dest)
    ok(f"前端已部署到 {html_target}")

# ===================== [7/8] 启动 admin =====================
PGREPV_CMD = """@echo off
rem Windows 下的 pgrep 兼容垫片：pgrep -c <name> 输出进程数
setlocal EnableExtensions
set "name=%~2"
if /i "%~1"=="-c" (
  for /f "delims=" %%A in ('tasklist /FI "IMAGENAME eq %name%.exe" /NH 2^>nul ^| find /c /i "%name%"') do @echo %%A
) else (
  tasklist /FI "IMAGENAME eq %name%.exe" /NH 2>nul | find /i "%name%"
)
"""

def _patch_admin_app():
    """让 admin/app.py 兼容 Windows：
    1) psql 子进程输出按 UTF-8 解码（默认 GBK 会炸）；
    2) /api/info 的 pgrep 在 Windows 不存在，改用 tasklist 统计。"""
    app = ADMIN_DIR/"app.py"
    if not app.exists():
        return
    src = app.read_text(encoding="utf-8")
    orig = src
    src = src.replace(
        "r = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=60)",
        "r = subprocess.run(cmd, env=env, capture_output=True, text=True,\n"
        "                           encoding='utf-8', errors='replace', timeout=60)")
    if "tasklist', '/FI', 'IMAGENAME eq nginx.exe'" not in src:
        src = src.replace(
            "nproc = subprocess.run(['pgrep', '-c', 'nginx'], capture_output=True, text=True)",
            "nr = subprocess.run(['tasklist', '/FI', 'IMAGENAME eq nginx.exe', '/NH'],\n"
            "                                    capture_output=True, text=True,\n"
            "                                    encoding='utf-8', errors='replace')\n"
            "        nproc = type('R', (), {'stdout': str(sum(1 for _ln in nr.stdout.splitlines() if 'nginx' in _ln.lower()))})()")
    if src != orig:
        app.write_text(src, encoding="utf-8")
        ok("已为 admin/app.py 应用 Windows 兼容补丁")

def step_admin():
    log("==== [7/8] 启动本地管理台 admin ====")
    if SKIP_ADMIN: log("跳过"); return
    app = ADMIN_DIR/"app.py"
    if not app.exists(): warn("admin\\app.py 不存在，跳过"); return
    # ---- Windows 兼容配置：admin 默认按 Linux 环境设计（pg_db=postgres、
    #      deploy_dir=/opt/...、依赖 PATH 里的 psql/pgrep）。这里生成
    #      config.local.json + pgrep 垫片，使其指向本机安装。 ----
    try:
        cfg = {
            "pg_host": PG_HOST, "pg_port": str(PG_PORT), "pg_db": PG_DBNAME,
            "pg_user": PG_SUPERUSER, "pg_password": PG_APP_PASSWORD or "",
            "deploy_dir": f"{NGINX_PREFIX}\\html\\ngxdb".replace("\\", "\\\\"),
            "reload_url": f"http://{PG_HOST}:{LISTEN_PORT}/func/_reloadfunctions",
        }
        (ADMIN_DIR/"config.local.json").write_text(
            json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
        ok(f"已写入 admin 本机配置: {ADMIN_DIR/'config.local.json'}")
    except Exception as e:
        warn(f"写入 admin 配置失败: {e}")
    try:
        pgrepc = ADMIN_DIR/"pgrep.cmd"
        if not pgrepc.exists():
            pgrepc.write_text(PGREPV_CMD, encoding="gbk")
    except Exception as e:
        warn(f"写入 pgrep 垫片失败: {e}")
    _patch_admin_app()
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("127.0.0.1", 8765)); s.close()
    except OSError:
        log("端口 8765 被占用，先清理旧 admin 进程")
        try:
            subprocess.run('for /f "tokens=5" %a in (\'netstat -ano ^| findstr :8765\') do taskkill /F /PID %a',
                           shell=True, capture_output=True, text=True)
        except Exception: pass
        time.sleep(0.5)
    env = os.environ.copy()
    pgbin = pg_bin_dir()
    extra_path = [str(p) for p in (pgbin, ADMIN_DIR) if p]
    env["PATH"] = os.pathsep.join(extra_path + [env.get("PATH", "")])
    env["PGPASSWORD"] = PG_APP_PASSWORD or ""
    env["NGXDB_DEPLOY_DIR"] = f"{NGINX_PREFIX}\\html\\ngxdb"
    env["ADMIN_PORT"] = "8765"
    log(f"以子进程方式启动 admin: {sys.executable} {app}")
    subprocess.Popen([sys.executable, str(app)],
                     cwd=str(ADMIN_DIR), env=env,
                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(1.5)
    try:
        r = urllib.request.urlopen("http://127.0.0.1:8765/api/ping", timeout=5)
        if r.status == 200: ok("admin 已启动: http://127.0.0.1:8765/")
        else: warn(f"admin 响应异常 HTTP {r.status}")
    except Exception as e:
        warn(f"admin /api/ping 验证失败: {e}")

# ===================== [8/8] 启动 nginx + 验证 =====================
def step_start_verify():
    log("==== [8/8] 启动 nginx + 验证登录 ====")
    target = Path(NGINX_PREFIX)
    exe = target/"nginx.exe"
    if not exe.exists(): raise SystemExit(f"未找到 {exe}")
    conf = target/"conf"/"nginx.conf"
    try:
        subprocess.run([str(exe), "-s", "stop"], capture_output=True, text=True,
                       cwd=str(target), timeout=15)
        time.sleep(1)
    except Exception: pass
    log("校验 nginx -t ...")
    r = subprocess.run([str(exe), "-t", "-c", str(conf), "-p", str(target)+"\\"],
                       capture_output=True, text=True, cwd=str(target))
    if r.returncode != 0 and "postgres" in (r.stdout+r.stderr):
        warn("官方 nginx.exe 不支持 postgres 指令，自动改用降级 nginx.conf ...")
        conf, _ = _write_nginx_conf(use_module=False)
        r = subprocess.run([str(exe), "-t", "-c", str(conf), "-p", str(target)+"\\"],
                           capture_output=True, text=True, cwd=str(target))
    if r.returncode != 0:
        err((r.stdout+r.stderr).strip() or "(无输出)")
        raise SystemExit("nginx -t 失败，无法启动")
    log("启动 nginx ...")
    subprocess.Popen([str(exe), "-c", str(conf), "-p", str(target)+"\\"],
                     cwd=str(target),
                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    ok("nginx 已启动")
    time.sleep(1)
    if SKIP_VERIFY: log("跳过登录验证(--skip-verify)"); return
    try:
        r = urllib.request.urlopen(f"http://127.0.0.1:{LISTEN_PORT}/login.html", timeout=5)
        ok(f"GET /login.html -> HTTP {r.status}")
    except Exception as e:
        warn(f"GET /login.html 失败: {e}")
    try:
        data = urllib.parse.urlencode({"loginname":"admin","pass":"123456","system":"100"}).encode()
        req = urllib.request.Request(f"http://127.0.0.1:{LISTEN_PORT}/func/sysinfo/login",
                                    data=data, method="POST")
        req.add_header("Content-Type", "application/x-www-form-urlencoded")
        resp = urllib.request.urlopen(req, timeout=10).read().decode("utf-8","replace")
        if '"errorcode":0' in resp and "token" in resp:
            ok("登录验证成功！默认账号 admin/123456")
        else:
            warn(f"/func/sysinfo/login 响应: {resp[:300]}")
            warn("（若 nginx 为官方二进制、未编译自研模块，/func 网关不可用，但前端/数据库/管理台仍可用）")
    except Exception as e:
        warn(f"/func/sysinfo/login 验证失败: {e}")

# ===================== C/C++ 编译工具检测与安装 =====================
# 自研 postgres 模块需编译进 nginx.exe。官方 Windows 二进制不含该模块，
# 编译需要 MSVC（cl.exe）或 MinGW-w64（gcc.exe）。此处提供检测与一键安装。

def find_msvc():
    """检测 MSVC 编译器（cl.exe）。返回 cl.exe 路径或 None"""
    candidates = []
    vswhere = Path(r"C:\Program Files (x86)\Microsoft Visual Studio\Installer\vswhere.exe")
    if vswhere.exists():
        try:
            r = subprocess.run(
                [str(vswhere), "-latest", "-products", "*",
                 "-requires", "Microsoft.VisualStudio.Component.VC.Tools.x86.x64",
                 "-property", "installationPath"],
                capture_output=True, text=True, timeout=30)
            candidates += [l.strip() for l in r.stdout.splitlines() if l.strip()]
        except Exception:
            pass
    for base in (r"C:\Program Files (x86)\Microsoft Visual Studio",
                 r"C:\Program Files\Microsoft Visual Studio"):
        b = Path(base)
        if b.exists():
            try:
                candidates += [str(p) for p in b.iterdir() if p.is_dir()]
            except Exception:
                pass
    for root in candidates:
        rp = Path(root)
        if not rp.exists():
            continue
        try:
            hits = sorted(rp.glob("VC/Tools/MSVC/*/bin/Hostx64/x64/cl.exe"))
        except Exception:
            hits = []
        if hits:
            return str(hits[-1])
    return None

def find_mingw():
    """检测 MinGW-w64 编译器（gcc.exe）。返回 bin 目录或 None"""
    p = shutil.which("gcc.exe") or shutil.which("x86_64-w64-mingw32-gcc.exe")
    if p:
        return str(Path(p).parent)
    for d in (SCRIPT_DIR/"mingw64"/"bin", SCRIPT_DIR/"w64devkit"/"bin",
              Path(r"C:\mingw64\bin"), Path(r"C:\msys64\mingw64\bin"),
              Path(r"C:\MinGW\bin")):
        if (d/"gcc.exe").exists():
            return str(d)
    return None

def tool_status():
    return {"msvc": find_msvc(), "mingw": find_mingw()}

def install_mingw():
    """下载 WinLibs MinGW-w64 便携 zip，解压到脚本目录（无需管理员）"""
    import json
    log("安装 MinGW-w64（WinLibs 便携版，解压即用）...")
    api = "https://api.github.com/repos/brechtsanders/winlibs_mingw/releases/latest"
    req = urllib.request.Request(api, headers={"User-Agent": HTTP_UA,
                                               "Accept": "application/vnd.github+json"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        rel = json.loads(resp.read().decode("utf-8"))
    assets = [a for a in rel.get("assets", [])
              if a.get("name", "").endswith(".zip")
              and "x86_64" in a["name"] and "posix" in a["name"]
              and "ucrt" in a["name"] and "llvm" not in a["name"]]
    if not assets:
        raise RuntimeError("WinLibs 最新 release 中未找到 x86_64 posix ucrt 的 zip 包")
    asset = min(assets, key=lambda a: a.get("size", 0))
    log(f"选定 {asset['name']} ({asset.get('size', 0)//1024//1024} MB)")
    z = download(asset["browser_download_url"], DOWNLOAD_DIR/asset["name"],
                 "MinGW-w64 (WinLibs)")
    log(f"解压 {z.name} -> {SCRIPT_DIR}")
    _extract_zip_progress(z, SCRIPT_DIR)
    gcc = SCRIPT_DIR/"mingw64"/"bin"/"gcc.exe"
    if not gcc.exists():
        for p in SCRIPT_DIR.glob("*/bin/gcc.exe"):
            gcc = p
            break
    if not gcc.exists():
        raise RuntimeError(f"解压完成但找不到 gcc.exe（检查 {SCRIPT_DIR}）")
    ok(f"MinGW-w64 已安装: {gcc.parent.parent}")
    return str(gcc.parent)

def install_msvc():
    """下载并静默安装 Visual Studio Build Tools（MSVC），需管理员权限"""
    log("安装 Microsoft Visual Studio Build Tools (MSVC) ...")
    log("注意：下载约 4MB 引导器，安装约 2-4GB、耗时 10-30 分钟，需要管理员权限。")
    url = "https://aka.ms/vs/17/release/vs_BuildTools.exe"
    dest = DOWNLOAD_DIR/"vs_BuildTools.exe"
    download(url, dest, "VS Build Tools 引导器")
    args = [str(dest), "--quiet", "--wait", "--norestart",
            "--add", "Microsoft.VisualStudio.Workload.VCTools",
            "--includeRecommended"]
    log("静默安装中（请耐心等待，不要关闭本程序）...")
    with open(DOWNLOAD_DIR/"vs_buildtools_install.log", "ab") as lf:
        r = subprocess.run(args, stdout=lf, stderr=subprocess.STDOUT, timeout=7200)
    if r.returncode != 0:
        raise RuntimeError(
            f"Build Tools 安装失败，退出码 {r.returncode}"
            f"（详见 {DOWNLOAD_DIR/'vs_buildtools_install.log'}）")
    cl = find_msvc()
    if not cl:
        raise RuntimeError("Build Tools 安装完成但未找到 cl.exe（可能缺少 VC 工具集）")
    ok(f"MSVC 已安装: {cl}")
    return cl

# ===================== 卸载 =====================
UNINSTALL_ITEMS = [
    ("nginx",  "nginx 服务（停止进程并删除 nginx 目录）"),
    ("db",     "数据库内容（删除 ngxdb 库与 gm/conn 角色）"),
    ("pg",     "PostgreSQL 程序（停止服务并删除 pgsql 目录）"),
    ("admin",  "管理台运行配置（config.local.json / pgrep.cmd / data）"),
    ("tools",  "编译工具与构建目录（mingw64、build，含 MSYS2）"),
    ("cache",  "下载缓存（downloads 目录）"),
    ("log",    "安装日志 install.log"),
]

def _stop_admin_proc():
    """停掉监听 8765 的 admin 进程（不误伤其他 python 进程）"""
    try:
        out = subprocess.run("netstat -ano | findstr :8765", shell=True,
                             capture_output=True, text=True, timeout=15).stdout
        pids = set()
        for ln in out.splitlines():
            if "LISTENING" in ln.upper():
                parts = ln.split()
                if parts and parts[-1].isdigit():
                    pids.add(parts[-1])
        for pid in pids:
            subprocess.run(["taskkill", "/F", "/PID", pid],
                           capture_output=True, timeout=15)
        if pids:
            log(f"已停止 admin 进程 (PID {','.join(sorted(pids))})")
    except Exception as e:
        warn(f"停止 admin 进程时出现问题（忽略）: {e}")

def do_uninstall(sel):
    """按选择卸载。sel: dict(key->bool)，key 见 UNINSTALL_ITEMS。
    顺序：先停 nginx/admin，再删库（需要 PG 还在），再停 PG、删目录。"""
    log("========== 开始卸载 ==========")
    # ---- nginx ----
    if sel.get("nginx"):
        log("卸载 nginx ...")
        _stop_nginx()
        _force_rmtree(Path(NGINX_PREFIX))
        ok(f"nginx 已卸载（目录 {NGINX_PREFIX} 已删除）")
    # ---- admin ----
    if sel.get("admin"):
        log("清理管理台运行配置 ...")
        _stop_admin_proc()
        for f in ("config.local.json", "pgrep.cmd"):
            p = ADMIN_DIR/f
            if p.exists():
                p.unlink(missing_ok=True)
        for d in ("data", "__pycache__"):
            p = ADMIN_DIR/d
            if p.exists():
                _force_rmtree(p)
        ok("管理台运行配置已清理（源码保留）")
    else:
        _stop_admin_proc()   # 即使不单独卸 admin，也先停掉，避免占用
    # ---- 数据库内容（要在 pgsql 目录删除之前做）----
    if sel.get("db"):
        bd = pg_bin_dir()
        if bd:
            log(f"删除数据库 {PG_DBNAME} 与角色 {PG_APP_USER}/conn ...")
            try:
                _ensure_pg_running()
                psql(bd, "-c", f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                               f"WHERE datname='{PG_DBNAME}' AND pid <> pg_backend_pid();",
                     db="postgres")
                psql(bd, "-c", f"DROP DATABASE IF EXISTS {PG_DBNAME} WITH (FORCE);",
                     db="postgres")
                for role in ("conn", PG_APP_USER):
                    psql(bd, "-c", f"DROP ROLE IF EXISTS {role};", db="postgres")
                ok("数据库与角色已删除")
            except Exception as e:
                warn(f"删除数据库/角色失败（忽略，继续卸载）: {e}")
        else:
            warn("未找到 PostgreSQL，跳过数据库内容删除")
    # ---- PostgreSQL ----
    if sel.get("pg"):
        log("卸载 PostgreSQL ...")
        bd = pg_bin_dir()
        if bd:
            try:
                subprocess.run([str(bd/"pg_ctl.exe"), "stop", "-D", str(bd.parent/"data")],
                               capture_output=True, timeout=60)
                ok("PostgreSQL 服务已停止")
            except Exception:
                pass
        _force_rmtree(PG_INSTALL_DIR)
        ok(f"PostgreSQL 已卸载（目录 {PG_INSTALL_DIR} 已删除）")
    # ---- 编译工具与构建目录 ----
    if sel.get("tools"):
        log("清理编译工具与构建目录 ...")
        for d in (SCRIPT_DIR/"mingw64", SCRIPT_DIR/"build"):
            if d.exists():
                _force_rmtree(d)
        ok("mingw64 与 build 已删除")
    # ---- 下载缓存 ----
    if sel.get("cache"):
        log("清理下载缓存 ...")
        if DOWNLOAD_DIR.exists():
            _force_rmtree(DOWNLOAD_DIR)
        ok("downloads 已删除")
    # ---- 日志 ----
    if sel.get("log"):
        if LOG_FILE.exists():
            LOG_FILE.unlink(missing_ok=True)
        print("install.log 已删除")
    ok("========== 卸载完成 ==========")

# ===================== 主流程 =====================
CHECK_TOOLS_ONLY = False
INSTALL_TOOL = None
UNINSTALL = None          # None / "select" / "all"

def _parse_args():
    global SKIP_PG, SKIP_NGINX, SKIP_DB, SKIP_HTML, SKIP_ADMIN, SKIP_VERIFY, GUI
    global CHECK_TOOLS_ONLY, INSTALL_TOOL, UNINSTALL
    # 默认走 GUI（有窗口 + 日志 + 开始按钮）；显式 --cli 才用纯终端流程
    GUI = not any(a in ("--cli", "--yes-text", "--no-gui") for a in sys.argv[1:])
    for a in sys.argv[1:]:
        if a == "--gui": GUI = True
        elif a in ("--cli", "--no-gui"): GUI = False
        elif a == "--yes": SKIP_VERIFY = True
        elif a == "--skip-pg": SKIP_PG = True
        elif a == "--skip-nginx": SKIP_NGINX = True
        elif a == "--skip-db": SKIP_DB = True
        elif a == "--skip-html": SKIP_HTML = True
        elif a == "--skip-admin": SKIP_ADMIN = True
        elif a == "--skip-verify": SKIP_VERIFY = True
        elif a == "--check-tools": CHECK_TOOLS_ONLY = True
        elif a == "--install-msvc": INSTALL_TOOL = "msvc"
        elif a == "--install-mingw": INSTALL_TOOL = "mingw"
        elif a == "--uninstall": UNINSTALL = "select"
        elif a == "--uninstall-all": UNINSTALL = "all"
        elif a in ("-h","--help"):
            print(__doc__); sys.exit(0)
        else:
            warn(f"忽略未知参数: {a}")

def _uninstall_cli(mode):
    """终端交互式卸载：逐项确认，或 --uninstall-all 全部卸载"""
    sel = {k: False for k, _ in UNINSTALL_ITEMS}
    if mode == "all":
        for k, _ in UNINSTALL_ITEMS:
            sel[k] = True
        print("将卸载以下全部内容：")
        for k, desc in UNINSTALL_ITEMS:
            print(f"  - {desc}")
        if not SKIP_VERIFY:
            a = input("确认全部卸载？[y/N] ").strip().lower()
            if a not in ("y", "yes"):
                warn("已取消卸载"); return
    else:
        print("请逐项确认要卸载的内容（直接回车 = 不卸载）：")
        for k, desc in UNINSTALL_ITEMS:
            a = input(f"  卸载 {desc}? [y/N] ").strip().lower()
            sel[k] = a in ("y", "yes")
        if not any(sel.values()):
            warn("未选择任何卸载项，已取消"); return
    do_uninstall(sel)


def _step(key, state):
    """通知 GUI 步骤状态；终端模式下 UI['step'] 为 None，静默忽略"""
    fn = UI.get("step")
    if fn:
        try:
            fn(key, state)
        except Exception:
            pass


def _run_pipeline():
    global GUI_MODE, LISTEN_PORT, PG_PORT, NGINX_VERSION, SKIP_VERIFY, SKIP_PG, SKIP_NGINX, SKIP_DB, SKIP_HTML, SKIP_ADMIN
    GUI_MODE = GUI
    _step("chk", "run")
    check_prereqs()
    _step("chk", "ok")

    if SKIP_PG:
        _step("pg", "skip"); _step("pgx", "skip")
    else:
        _step("pg", "run")
        installer, kind = step_download_pg()
        _step("pg", "ok")
        _step("pgx", "run")
        step_install_pg(installer, kind)
        _step("pgx", "ok")

    if SKIP_DB:
        _step("db", "skip")
    else:
        _step("db", "run")
        step_init_db(PG_APP_PASSWORD or "CHANGE_ME")
        _step("db", "ok")

    if SKIP_NGINX:
        _step("ng", "skip"); _step("conf", "skip")
    else:
        _step("ng", "run")
        step_download_nginx()
        _step("ng", "ok")
        _step("conf", "run")
        step_conf_html()
        _step("conf", "ok")

    if SKIP_ADMIN:
        _step("admin", "skip")
    else:
        _step("admin", "run")
        step_admin()
        _step("admin", "ok")

    _step("run", "run")
    if SKIP_VERIFY:
        _step("run", "skip")
    else:
        step_start_verify()
        _step("run", "ok")

    print()
    ok("================ NGXDB 安装完成 ================")
    ok(f"访问地址：http://<本机IP>:{LISTEN_PORT}/login.html")
    ok(f"默认账号：admin     密码：123456     (生产环境请立即修改！)")
    ok(f"数据库：{PG_DBNAME} (角色 {PG_APP_USER}/conn)")
    if PG_APP_PASSWORD:
        ok(f"数据库角色口令：{PG_APP_PASSWORD}（请妥善保存）")
    else:
        warn("数据库角色口令由环境变量指定；请妥善保存 PG_APP_PASSWORD")


def main():
    _parse_args()
    _flog("RUN", f"========== NGXDB 安装开始 (Python {sys.version.split()[0]}, GUI={bool(GUI)}) ==========")
    if UNINSTALL:
        if GUI: _run_uninstall_gui()
        else:   _uninstall_cli(UNINSTALL)
        return
    if CHECK_TOOLS_ONLY:
        st = tool_status()
        ok(f"MSVC : {st['msvc'] or '未安装'}")
        ok(f"MinGW: {st['mingw'] or '未安装'}")
        if not (st["msvc"] and st["mingw"]):
            print("缺少编译工具时可运行: py -3 install.py --install-msvc 或 --install-mingw")
        return
    if INSTALL_TOOL:
        (install_msvc if INSTALL_TOOL == "msvc" else install_mingw)()
        return
    if GUI: _run_with_gui()
    else: _run_pipeline()





# ===================== GUI install control panel =====================
def _run_with_gui():
    import tkinter as tk
    from tkinter import ttk, messagebox, simpledialog
    import queue

    log_q = queue.Queue()
    ctl_q = queue.Queue()
    ABORT = {"flag": False}

    class _Redir:
        """把 stdout 与 stderr 一并重定向到 GUI 日志面板（错误信息也实时可见）"""
        def __init__(self, q):
            self.q = q
            self._o = None
            self._e = None
        def __enter__(self):
            self._o, self._e = sys.stdout, sys.stderr
            sys.stdout = self
            sys.stderr = self
            return self
        def __exit__(self, *a):
            sys.stdout, sys.stderr = self._o, self._e
        def write(self, s):
            if s:
                self.q.put(s)
        def flush(self):
            pass

    ICON = {"wait": "[ ]", "run": "[>]", "ok": "[OK]", "skip": "[..]", "fail": "[XX]"}

    root = tk.Tk()
    root.title("NGXDB 安装向导")
    root.geometry("980x840")
    root.minsize(860, 720)
    root.update_idletasks()
    _sw, _sh = root.winfo_screenwidth(), root.winfo_screenheight()
    root.geometry(f"980x840+{max(0, (_sw-980)//2)}+{max(0, (_sh-840)//2 - 30)}")

    head = tk.Frame(root, bg="#2b5a9b", height=58)
    head.pack(fill="x")
    head.pack_propagate(False)
    tk.Label(head, text="NGXDB 安装向导", bg="#2b5a9b", fg="white",
             font=("Segoe UI", 15, "bold")).pack(side="left", padx=14)
    tk.Label(head, text=str(SCRIPT_DIR), bg="#2b5a9b", fg="#cfe0f7",
             font=("Consolas", 9)).pack(side="left")

    opt = ttk.LabelFrame(root, text="安装选项")
    opt.pack(fill="x", padx=12, pady=(10, 6))
    v_port = tk.StringVar(value=os.environ.get("LISTEN_PORT", "80"))
    v_pgport = tk.StringVar(value=os.environ.get("PG_PORT", "5432"))
    v_ngver = tk.StringVar(value=os.environ.get("NGINX_VERSION", NGINX_VERSION))
    v_skipv = tk.BooleanVar(value=False)
    row = 0
    ttk.Label(opt, text="nginx 端口：").grid(row=row, column=0, sticky="e", padx=4, pady=3)
    ttk.Entry(opt, textvariable=v_port, width=6).grid(row=row, column=1, sticky="w")
    ttk.Label(opt, text="PostgreSQL 端口：").grid(row=row, column=2, sticky="e", padx=(16, 4))
    ttk.Entry(opt, textvariable=v_pgport, width=6).grid(row=row, column=3, sticky="w")
    ttk.Label(opt, text="nginx 版本：").grid(row=row, column=4, sticky="e", padx=(16, 4))
    ttk.Entry(opt, textvariable=v_ngver, width=9).grid(row=row, column=5, sticky="w")
    ttk.Checkbutton(opt, text="跳过最终验证", variable=v_skipv).grid(row=row, column=6, padx=(16, 4))

    stf = ttk.LabelFrame(root, text="安装步骤")
    stf.pack(fill="x", padx=12, pady=6)
    step_vars = {}
    for i, (k, desc) in enumerate(STEPS):
        sv = tk.StringVar(value=ICON["wait"] + "  " + desc)
        step_vars[k] = sv
        ttk.Label(stf, textvariable=sv).grid(row=i, column=0, sticky="w", padx=6)
    prog = ttk.Progressbar(stf, mode="determinate", maximum=100)
    prog.grid(row=len(STEPS), column=0, sticky="ew", pady=(8, 4), padx=6)
    stf.columnconfigure(0, weight=1)

    # ---- Build tools 面板：检测/安装 MSVC / MinGW（编译 nginx postgres 模块用）----
    tf = ttk.LabelFrame(root, text="编译工具（编译 nginx 自研 postgres 模块用）")
    tf.pack(fill="x", padx=12, pady=6)
    v_msvc = tk.StringVar(value="MSVC：待检测")
    v_mingw = tk.StringVar(value="MinGW：待检测")
    ttk.Label(tf, textvariable=v_msvc, font=("Consolas", 9)).grid(
        row=0, column=0, sticky="w", padx=6, pady=2)
    ttk.Label(tf, textvariable=v_mingw, font=("Consolas", 9)).grid(
        row=1, column=0, sticky="w", padx=6, pady=2)
    btn_check = ttk.Button(tf, text="重新检测")
    btn_check.grid(row=0, column=1, rowspan=2, padx=6)
    btn_inst = ttk.Button(tf, text="安装缺失工具...")
    btn_inst.grid(row=0, column=2, rowspan=2, padx=(0, 6))
    tf.columnconfigure(0, weight=1)

    def check_tools(*_):
        st = tool_status()
        v_msvc.set("MSVC：" + (st["msvc"] or "未安装"))
        v_mingw.set("MinGW：" + (st["mingw"] or "未安装"))
        return st

    def choose_tool_dialog():
        """模态对话框：让用户选择安装 MSVC / MinGW / 跳过。返回 'msvc'|'mingw'|'skip'"""
        dlg = tk.Toplevel(root)
        dlg.title("需要 C/C++ 编译工具")
        dlg.geometry("560x240")
        dlg.resizable(False, False)
        dlg.grab_set()
        dlg.transient(root)
        var = tk.StringVar(value="mingw")
        ttk.Label(dlg, text="未检测到可用的 C/C++ 编译工具。\n"
                            "编译 nginx 自研 postgres 模块需要 MSVC 或 MinGW，请选择要安装的：",
                  wraplength=520, justify="left").pack(anchor="w", padx=14, pady=(12, 6))
        ttk.Radiobutton(dlg, variable=var, value="mingw",
                        text="MinGW-w64（WinLibs 便携版，zip 约 260MB，解压即用，推荐）"
                        ).pack(anchor="w", padx=22, pady=4)
        ttk.Radiobutton(dlg, variable=var, value="msvc",
                        text="MSVC Build Tools（微软官方，约 2-4GB，安装 10-30 分钟，需管理员）"
                        ).pack(anchor="w", padx=22, pady=4)
        ttk.Radiobutton(dlg, variable=var, value="skip",
                        text="跳过（暂不安装，直接继续安装 NGXDB）"
                        ).pack(anchor="w", padx=22, pady=4)
        result = {"v": "skip"}
        def _ok():
            result["v"] = var.get()
            dlg.destroy()
        btnrow = ttk.Frame(dlg)
        btnrow.pack(pady=10)
        ttk.Button(btnrow, text="确定", command=_ok).pack(side="left", padx=6)
        ttk.Button(btnrow, text="取消", command=dlg.destroy).pack(side="left", padx=6)
        dlg.wait_window()
        return result["v"]

    def install_tool_worker(choice):
        with _Redir(log_q):
            try:
                if choice == "msvc":
                    install_msvc()
                else:
                    install_mingw()
            except Exception as e:
                log_exc("编译工具安装失败")
                err(f"编译工具安装失败: {e}")
                ctl_q.put(("tools_fail", str(e)))
                return
            ctl_q.put(("tools_done", choice))

    def install_tool_async(choice):
        btn_inst.configure(state="disabled")
        btn_check.configure(state="disabled")
        status.configure(text=f"正在安装 {'MSVC' if choice=='msvc' else 'MinGW'} ...")
        threading.Thread(target=install_tool_worker, args=(choice,), daemon=True).start()

    def on_install_missing():
        st = check_tools()
        if st["msvc"] and st["mingw"]:
            messagebox.showinfo("编译工具", "MSVC 与 MinGW 均已安装，无需安装。")
            return
        if st["msvc"] or st["mingw"]:
            miss = "MinGW" if st["msvc"] else "MSVC"
            if not messagebox.askyesno("编译工具",
                    f"{miss} 未安装（另一个已存在）。是否安装 {miss}？"):
                return
            install_tool_async("mingw" if st["msvc"] else "msvc")
            return
        choice = choose_tool_dialog()
        if choice in ("msvc", "mingw"):
            install_tool_async(choice)

    logf = ttk.LabelFrame(root, text="安装日志")
    logf.pack(fill="both", expand=True, padx=12, pady=6)
    logtxt = tk.Text(logf, wrap="word", height=16, font=("Consolas", 9))
    sc = ttk.Scrollbar(logf, orient="vertical", command=logtxt.yview)
    logtxt.configure(yscrollcommand=sc.set, state="disabled")
    logtxt.pack(side="left", fill="both", expand=True, padx=(4, 0), pady=4)
    sc.pack(side="right", fill="y", pady=4)

    bar = ttk.Frame(root)
    bar.pack(fill="x", padx=12, pady=(4, 10))
    status = ttk.Label(bar, text="就绪")
    status.pack(side="left")

    def _open_url(url):
        import webbrowser
        status.configure(text=f"正在打开 {url}")
        webbrowser.open(url)

    # 安装完成后可一键打开的两个入口按钮
    btn_home = ttk.Button(bar, text="打开系统入口", state="disabled",
                          command=lambda: _open_url(f"http://127.0.0.1:{LISTEN_PORT}/login.html"))
    btn_home.pack(side="left", padx=(14, 4))
    btn_admin = ttk.Button(bar, text="打开管理台", state="disabled",
                           command=lambda: _open_url("http://127.0.0.1:8765/"))
    btn_admin.pack(side="left", padx=4)
    btn_uninst = ttk.Button(bar, text="卸载...",
                            command=lambda: subprocess.Popen(
                                [sys.executable, str(Path(__file__).resolve()),
                                 "--uninstall"]))
    btn_uninst.pack(side="left", padx=4)

    btn_start = ttk.Button(bar, text="开始安装")
    btn_start.pack(side="right")
    btn_cancel = ttk.Button(bar, text="取消", state="disabled")
    btn_cancel.pack(side="right", padx=(0, 8))

    def append_log(s):
        logtxt.configure(state="normal")
        logtxt.insert("end", s)
        logtxt.see("end")
        logtxt.configure(state="disabled")

    UI["step"] = lambda k, st: ctl_q.put(("step", (k, ICON.get(st, "?"))))
    UI["progress"] = lambda d, t, x: ctl_q.put(("progress", (d, t, x)))
    UI["confirm"] = lambda t, m: messagebox.askokcancel(t, m)
    UI["ask"] = lambda t, p, d="": (simpledialog.askstring(t, p, initialvalue=d) or d)

    def worker(pending_tool=None):
        global LISTEN_PORT, PG_PORT, NGINX_VERSION, SKIP_VERIFY
        try:
            os.environ["LISTEN_PORT"] = (v_port.get().strip() or "80")
            os.environ["PG_PORT"] = (v_pgport.get().strip() or "5432")
            os.environ["NGINX_VERSION"] = (v_ngver.get().strip() or NGINX_VERSION)
            LISTEN_PORT = os.environ["LISTEN_PORT"]
            PG_PORT = os.environ["PG_PORT"]
            NGINX_VERSION = os.environ["NGINX_VERSION"]
            SKIP_VERIFY = bool(v_skipv.get())

            with _Redir(log_q):
                if pending_tool in ("msvc", "mingw"):
                    try:
                        (install_msvc if pending_tool == "msvc"
                         else install_mingw)()
                    except Exception as e:
                        err(f"编译工具安装失败（继续安装 NGXDB）: {e}")
                    check_tools()
                _run_pipeline()
            ctl_q.put(("done", None))
        except SystemExit as e:
            log_exc("GUI 安装流程被终止(SystemExit)")
            ctl_q.put(("error", str(e) or "已停止"))
        except Exception as e:
            log_exc("GUI 安装流程异常")
            ctl_q.put(("error", str(e)))

    def start():
        ABORT["flag"] = False
        btn_start.configure(state="disabled")
        btn_cancel.configure(state="normal")
        status.configure(text="正在安装 ...")
        for k, _ in STEPS:
            ctl_q.put(("step", (k, ICON["wait"])))
        prog.configure(value=0)
        pending_tool = None
        st = tool_status()
        if not (st["msvc"] and st["mingw"]):
            choice = choose_tool_dialog()
            if choice in ("msvc", "mingw"):
                pending_tool = choice
        threading.Thread(target=worker, args=(pending_tool,), daemon=True).start()

    def cancel():
        ABORT["flag"] = True
        btn_cancel.configure(state="disabled")
        status.configure(text="已请求取消（详见日志）")

    btn_start.configure(command=start)
    btn_cancel.configure(command=cancel)
    btn_check.configure(command=check_tools)
    btn_inst.configure(command=on_install_missing)
    check_tools()   # 打开窗口即检测编译工具

    def pump():
        try:
            while True:
                append_log(log_q.get_nowait())
        except queue.Empty:
            pass
        try:
            while True:
                kind, payload = ctl_q.get_nowait()
                if kind == "step":
                    key, icon = payload
                    if key in step_vars:
                        desc = dict(STEPS).get(key, "")
                        step_vars[key].set(icon + "  " + desc)
                elif kind == "progress":
                    done, total, text = payload
                    prog.configure(value=(100 * done // total) if total else 0)
                    status.configure(text=text)
                elif kind == "done":
                    prog.configure(value=100)
                    status.configure(text="安装完成")
                    btn_start.configure(state="normal", text="再次安装")
                    btn_cancel.configure(state="disabled")
                    btn_home.configure(state="normal",
                        command=lambda: _open_url(f"http://127.0.0.1:{LISTEN_PORT}/login.html"))
                    btn_admin.configure(state="normal")
                    messagebox.showinfo("安装完成",
                        "NGXDB 安装完成！\n\n"
                        f"系统入口：http://127.0.0.1:{LISTEN_PORT}/login.html\n"
                        "管理台：http://127.0.0.1:8765/\n\n"
                        "默认账号：admin / 123456\n（两个入口按钮已启用，也可直接点击打开）")
                elif kind == "error":
                    status.configure(text="安装失败（详见 install.log）")
                    btn_start.configure(state="normal")
                    btn_cancel.configure(state="disabled")
                    messagebox.showerror("安装失败",
                        f"{payload}\n\n完整错误堆栈已写入:\n{LOG_FILE}")
                elif kind == "tools_done":
                    check_tools()
                    btn_inst.configure(state="normal")
                    btn_check.configure(state="normal")
                    status.configure(text="编译工具已就绪")
                    messagebox.showinfo("编译工具", "编译工具安装完成。")
                elif kind == "tools_fail":
                    check_tools()
                    btn_inst.configure(state="normal")
                    btn_check.configure(state="normal")
                    status.configure(text="编译工具安装失败")
                    messagebox.showerror("编译工具",
                        f"编译工具安装失败:\n{payload}\n\n详情见 {LOG_FILE}")
        except queue.Empty:
            pass
        root.after(120, pump)

    pump()
    root.mainloop()



# ===================== 卸载 GUI =====================
def _run_uninstall_gui():
    """独立卸载窗口：可勾选卸载项，也支持一键全部卸载"""
    import tkinter as tk
    from tkinter import ttk, messagebox
    import queue

    q = queue.Queue()

    class _Redir:
        def __init__(self, q): self.q = q
        def write(self, s):
            if s: self.q.put(s)
        def flush(self): pass

    root = tk.Tk()
    root.title("NGXDB 卸载")
    root.geometry("900x700")
    root.minsize(800, 620)
    root.update_idletasks()
    _sw, _sh = root.winfo_screenwidth(), root.winfo_screenheight()
    root.geometry(f"900x700+{max(0, (_sw-900)//2)}+{max(0, (_sh-700)//2 - 30)}")

    head = tk.Frame(root, bg="#8b3a3a", height=52)
    head.pack(fill="x")
    head.pack_propagate(False)
    tk.Label(head, text="NGXDB 卸载", bg="#8b3a3a", fg="white",
             font=("Segoe UI", 14, "bold")).pack(side="left", padx=14)
    tk.Label(head, text=str(SCRIPT_DIR), bg="#8b3a3a", fg="#f2d7d7",
             font=("Consolas", 9)).pack(side="left")

    box = ttk.LabelFrame(root, text="选择要卸载的内容")
    box.pack(fill="x", padx=12, pady=(10, 6))
    vars_ = {}
    for i, (k, desc) in enumerate(UNINSTALL_ITEMS):
        v = tk.BooleanVar(value=False)
        vars_[k] = v
        ttk.Checkbutton(box, text=desc, variable=v).grid(
            row=i, column=0, sticky="w", padx=10, pady=3)
    box.columnconfigure(0, weight=1)

    def select_all():
        on = bool(v_all.get())
        for v in vars_.values():
            v.set(on)
    v_all = tk.BooleanVar(value=False)
    ttk.Checkbutton(box, text="全部卸载（勾选以下所有项）", variable=v_all,
                    command=select_all).grid(row=len(UNINSTALL_ITEMS),
                                             column=0,
                                             sticky="w", padx=10, pady=3)

    lf = ttk.LabelFrame(root, text="卸载日志")
    lf.pack(fill="both", expand=True, padx=12, pady=6)
    txt = tk.Text(lf, wrap="word", height=10, font=("Consolas", 9))
    sc = ttk.Scrollbar(lf, orient="vertical", command=txt.yview)
    txt.configure(yscrollcommand=sc.set, state="disabled")
    txt.pack(side="left", fill="both", expand=True, padx=(4, 0), pady=4)
    sc.pack(side="right", fill="y", pady=4)

    bar = ttk.Frame(root)
    bar.pack(fill="x", padx=12, pady=(4, 10))
    status = ttk.Label(bar, text="就绪（卸载不可恢复，请确认勾选项）")
    status.pack(side="left")

    def append(s):
        txt.configure(state="normal")
        txt.insert("end", s)
        txt.see("end")
        txt.configure(state="disabled")

    def worker():
        sel = {k: bool(v.get()) for k, v in vars_.items()}
        old_out, old_err = sys.stdout, sys.stderr
        sys.stdout = sys.stderr = _Redir(q)
        try:
            do_uninstall(sel)
            q.put("__DONE__")
        except Exception as e:
            log_exc("卸载失败")
            err(f"卸载失败: {e}")
            q.put("__FAIL__")
        finally:
            sys.stdout, sys.stderr = old_out, old_err

    def start():
        if not any(bool(v.get()) for v in vars_.values()):
            messagebox.showwarning("卸载", "请先勾选要卸载的内容。")
            return
        if not messagebox.askyesno(
                "确认卸载",
                "卸载操作不可恢复！\n\n将删除选中的目录/数据库/配置，"
                "并停止相关服务。\n确定继续吗？"):
            return
        btn_go.configure(state="disabled")
        for cb in cbs:
            cb.configure(state="disabled")
        cb_all.configure(state="disabled")
        status.configure(text="正在卸载 ...")
        threading.Thread(target=worker, daemon=True).start()

    btn_go = ttk.Button(bar, text="开始卸载", command=start)
    btn_go.pack(side="right")
    cbs = [w for w in box.winfo_children() if isinstance(w, ttk.Checkbutton)
           and w.cget("text") != "全部卸载（勾选以下所有项）"]
    cb_all = [w for w in box.winfo_children() if isinstance(w, ttk.Checkbutton)
              and w.cget("text") == "全部卸载（勾选以下所有项）"][0]

    def pump():
        try:
            while True:
                s = q.get_nowait()
                if s == "__DONE__":
                    status.configure(text="卸载完成")
                    btn_go.configure(state="normal", text="再次执行")
                    for cb in cbs: cb.configure(state="normal")
                    cb_all.configure(state="normal")
                    messagebox.showinfo("卸载", "卸载完成。\n\n重新安装可直接再次运行 install.py 或 安装.bat。")
                elif s == "__FAIL__":
                    status.configure(text="卸载失败（详见日志）")
                    btn_go.configure(state="normal")
                    for cb in cbs: cb.configure(state="normal")
                    cb_all.configure(state="normal")
                    messagebox.showerror("卸载", f"卸载过程中出错，详情见日志。\n{s}")
                else:
                    append(s)
        except queue.Empty:
            pass
        root.after(120, pump)

    pump()
    root.mainloop()


if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        code, msg = e.code, ""
        if isinstance(code, str):
            msg, code = code, 1
        elif code is None:
            code = 0
        elif not isinstance(code, int):
            code = 1
        if msg:
            err(msg)
        sys.exit(code)
    except KeyboardInterrupt:
        err("已手动中断")
        sys.exit(130)
    except Exception as e:
        log_exc("安装失败（未捕获异常）")
        err("安装失败: %s" % (e,))
        import traceback
        traceback.print_exc()
        sys.exit(1)

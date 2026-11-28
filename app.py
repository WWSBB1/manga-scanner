#!/usr/bin/env python3
"""漫画管理 APP (纯 HTML 版)

用 Python 内置 http.server 提供轻量 Web 界面,手机浏览器秒开。
后端逻辑(scanner.py / fileops.py)完全复用,不依赖 Flet。

启动: python3 app.py   浏览器开 http://127.0.0.1:8951
环境变量: MANGA_DB / MANGA_PORT
"""

import json
import logging
import os
import re
import sqlite3
import zipfile
import threading
import time
import concurrent.futures
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from urllib.parse import urlparse, parse_qs

BASE = os.path.dirname(os.path.abspath(__file__))

def _setup_runtime_logging():
    try:
        d = os.path.join(BASE, "logs")
        os.makedirs(d, exist_ok=True)
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s %(message)s",
            handlers=[
                logging.FileHandler(os.path.join(d, "runtime.log"), encoding="utf-8"),
            ],
        )
    except Exception:
        pass

_setup_runtime_logging()

import sys as _sys
_sys.excepthook = None

def _excepthook(exc_type, exc, tb):
    try:
        logging.getLogger("exception").error(
            "Uncaught exception", exc_info=(exc_type, exc, tb))
    except Exception:
        pass
    try:
        sys.__excepthook__(exc_type, exc, tb)
    except Exception:
        pass

_sys.excepthook = _excepthook

def _thread_excepthook(args):
    try:
        logging.getLogger("thread-exception").error(
            "Uncaught thread exception: %s", getattr(args, "thread", None),
            exc_info=(args.exc_type, args.exc_value, args.exc_traceback))
    except Exception:
        pass

if hasattr(threading, "excepthook"):
    threading.excepthook = _thread_excepthook


def write_op_log(action, detail=None):
    """记录关键操作到 logs/operations.log。"""
    try:
        d = os.path.join(BASE, "logs")
        os.makedirs(d, exist_ok=True)
        line = time.strftime("%Y-%m-%d %H:%M:%S") + " " + str(action)
        if detail:
            line += " " + json.dumps(detail, ensure_ascii=False, default=str)
        with open(os.path.join(d, "operations.log"), "a", encoding="utf-8") as f:
            f.write(line + "\n")
        logging.getLogger("operations").info(line)
    except Exception:
        pass


def write_api_log(kind, detail=None):
    """所有 API 请求/响应/错误统一写 logs/api.log。"""
    try:
        d = os.path.join(BASE, "logs")
        os.makedirs(d, exist_ok=True)
        line = time.strftime("%Y-%m-%d %H:%M:%S") + " " + str(kind)
        if detail:
            line += " " + json.dumps(detail, ensure_ascii=False,
                                     default=str)[:1600]
        with open(os.path.join(d, "api.log"), "a", encoding="utf-8") as f:
            f.write(line + "\n")
        logging.getLogger("api").info(line)
    except Exception:
        pass


CLIENT_LOG_BUF = []

THUMB_POOL = concurrent.futures.ThreadPoolExecutor(
    max_workers=4, thread_name_prefix="reader-thumb")


def flush_client_logs():
    if not CLIENT_LOG_BUF:
        return
    batch = CLIENT_LOG_BUF[:]
    del CLIENT_LOG_BUF[:len(batch)]
    for item in batch:
        write_api_log("CLIENT", item)


import sys
sys.path.insert(0, BASE)

import scanner
import fileops
import plugin_loader
import source as src_layer
import reader

DB_PATH = os.environ.get("MANGA_DB",
                        os.path.join(BASE, "manga.db"))
PORT = int(os.environ.get("MANGA_PORT", "8951"))
THUMB_PORT = int(os.environ.get("MANGA_THUMB_PORT", str(PORT + 1)))

FILTER_FIELDS = [
    ("artist", "作者"), ("group_name", "社团"), ("parody", "原作"),
    ("character", "角色"), ("language", "语言"), ("category", "分类"),
    ("tags", "标签包含"), ("title", "标题包含"),
]
FIELD_CN = {"name": "文件名", "artist": "作者", "parody": "原作",
            "language": "语言", "page_count": "页数", "tags": "标签",
            "title": "标题", "group_name": "社团", "character": "角色",
            "category": "分类", "format": "格式", "translated": "翻译",
            "source_url": "来源链接", "source_gid": "来源ID",
            "year": "年", "month": "月", "day": "日",
            "title_original": "原标题", "other_fields": "其它字段",
            "image_count": "图片数", "size": "大小", "has_meta": "有元数据",
            "error": "错误", "scanned_at": "扫描时间",
            "entry_count": "条目数", "id": "ID", "archive_id": "归档ID"}

SCAN_WORKERS = {}   # scan_id -> {running, stop, log[], stats}
CLS_WORKERS = {}    # cls_id -> {running, stop, log[], result}
UPLOAD_PLANS = {}   # plan_id -> prepare 结果
UPLOAD_WORKERS = {} # upload_id -> {running, log[], result}

# 最近一次扫描扫到的文件名。浏览页默认只看这批 ——
# 库里可能堆着几万条历史记录, 但用户此刻关心的是"刚扫的这批"。
# 不用 scanned_at 判断: 增量跳过的记录不会刷新它, 靠它筛不出"本次扫到"。
LAST_SCAN = {"names": None, "dirs": [], "at": None}

# 阅读页目录清单缓存: key=src_dir, value={name: 完整路径}。
# 进页面/改筛选只查库, 不重新列目录; 只有点「刷新」才重扫并覆盖。
READER_DIR_CACHE = {}
READER_DIR_LOCK = threading.Lock()


def fmt_size(n):
    if not n:
        return ""
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.0f}{unit}" if unit == "B" else f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}TB"


def dir_usable(path):
    """源/目标目录能不能用: 本地看目录是否存在, 远程交给数据源。

    远程路径 `123:/a/b` 在本机根本不存在, 不能用 os.path.isdir 判。
    """
    if src_layer.is_remote(path):
        return True
    return os.path.isdir(path)


def remote_note(path):
    """远程路径的能力说明, 给界面/接口提示用。"""
    if not src_layer.is_remote(path):
        return None
    return {"remote": True, "supports_copy": False,
            "note": "远程模式: 仅支持移动, 不支持复制(123 云盘 WebDAV "
                    "的 COPY 返回 500)"}


# ================================================================ API

class API:
    @staticmethod
    def upload_prepare(local_dir, recursive, target_root, compare_root):
        """扫描本地 zip, 与远程目录缓存比对, 生成上传/删除计划。"""
        if not local_dir or not os.path.isdir(local_dir):
            return {"error": "本地路径不存在"}
        if not target_root or not src_layer.is_remote(target_root):
            return {"error": "上传目标必须是 WebDAV 远程路径"}
        if not compare_root or not src_layer.is_remote(compare_root):
            return {"error": "远程比对范围必须是 WebDAV 远程路径"}

        targets, _unsupported = scanner._collect_targets(
            [local_dir], scanner.walk_workers([local_dir]), None,
            recursive=bool(recursive))

        # 先处理本地文件名末尾的 (1)/(2) 序号: 直接重命名去掉序号。
        # 若去掉序号后的目标文件已存在, 当前带序号文件视为重复, 进删除列表。
        suffix_re = re.compile(r"(\s*\(\d{1,2}\))+(\.[^.]+)$")
        renamed, delete = [], []
        items = []
        for path in targets:
            name = os.path.basename(path)
            m = suffix_re.search(name)
            if m:
                base_name = suffix_re.sub(m.group(2), name)
                base_path = os.path.join(os.path.dirname(path), base_name)
                if os.path.exists(base_path):
                    try:
                        size = os.path.getsize(path)
                    except OSError:
                        size = 0
                    delete.append({"path": path, "name": name, "size": size,
                                   "action": "delete_local",
                                   "reason": "本地序号重复, 无序号文件已存在"})
                    continue
                try:
                    os.rename(path, base_path)
                    renamed.append((name, base_name))
                    path, name = base_path, base_name
                except OSError as ex:
                    write_op_log("上传序号文件重命名失败",
                                 {"path": path, "error": str(ex)})
            try:
                size = os.path.getsize(path)
            except OSError:
                size = 0
            items.append({"path": path, "name": name, "size": size})
        if renamed:
            write_op_log("上传前重命名序号文件", {"renamed": renamed})

        def local_has_meta(path):
            try:
                with zipfile.ZipFile(path) as zf:
                    names = zf.namelist()
                found, _extra, _images = scanner.classify_entries(names)
                return bool(found)
            except Exception:
                return False

        for it in items:
            it["local_has_meta"] = local_has_meta(it["path"])

        conn = sqlite3.connect(DB_PATH)
        try:
            rows = conn.execute(
                "SELECT root,files_json FROM dir_listing WHERE root=? OR root LIKE ?",
                (compare_root, compare_root.rstrip("/") + "/%")).fetchall()
        finally:
            conn.close()
        remote = {}
        for root, fj in rows:
            prefix = ""
            if root.rstrip("/") != compare_root.rstrip("/"):
                prefix = root[len(compare_root.rstrip("/")):].strip("/") + "/"
            for x in json.loads(fj or "[]"):
                nm = x.get("name")
                if not nm:
                    continue
                remote.setdefault(nm, []).append({
                    "name": nm, "size": int(x.get("size") or 0),
                    "path": prefix + nm, "root": root,
                })

        conn = sqlite3.connect(DB_PATH)
        try:
            meta_names = {r[0] for r in conn.execute(
                "SELECT name FROM archives WHERE has_meta=1 AND error IS NULL")}
        finally:
            conn.close()

        upload, no_meta = [], []
        for it in items:
            if not it.get("local_has_meta"):
                no_meta.append(dict(it, action="pending",
                                    reason="本地文件无元数据"))
                continue
            candidates = remote.get(it["name"], [])
            if not candidates:
                upload.append(dict(it, action="upload", reason="远程不存在"))
                continue
            same = [r for r in candidates if r["size"] == it["size"]]
            if same:
                delete.append(dict(it, action="delete_local",
                                   reason="远程同名同大小", remote=same[0]))
                continue
            if it["name"] in meta_names:
                delete.append(dict(it, action="delete_local",
                                   reason="数据库已有元数据",
                                   remote=candidates[0]))
                continue
            upload.append(dict(it, action="replace_remote",
                               reason="远程同名不同大小且无元数据",
                               remote=candidates[0]))

        plan_id = f"upload_plan_{int(time.time() * 1000)}"
        UPLOAD_PLANS[plan_id] = {
            "local_dir": local_dir, "recursive": bool(recursive),
            "target_root": target_root, "compare_root": compare_root,
            "upload": upload, "delete": delete, "no_meta": no_meta,
        }
        write_op_log("上传比对", {"plan_id": plan_id,
                              "local_count": len(items),
                              "upload": len(upload), "delete": len(delete),
                              "no_meta": len(no_meta)})
        return {"plan_id": plan_id, "local_count": len(items),
                "remote_count": len(remote),
                "renamed_count": len(renamed),
                "upload_count": len(upload), "delete_count": len(delete),
                "no_meta_count": len(no_meta),
                "upload": upload, "delete": delete, "no_meta": no_meta}

    @staticmethod
    def upload_start(plan_id):
        plan = UPLOAD_PLANS.get(plan_id)
        if not plan:
            return {"error": "无效 plan_id"}
        uid = f"upload_{int(time.time() * 1000)}"
        UPLOAD_WORKERS[uid] = {"running": True, "log": [], "result": None,
            "progress": {"phase": "准备", "done": 0, "total": 0, "current": ""}}

        def log(msg):
            UPLOAD_WORKERS[uid]["log"].append(msg)
            write_api_log("UPLOAD", {"upload_id": uid, "msg": msg})

        def progress(phase=None, done=None, total=None, current=None):
            p = UPLOAD_WORKERS[uid]["progress"]
            if phase is not None: p["phase"] = phase
            if done is not None: p["done"] = done
            if total is not None: p["total"] = total
            if current is not None: p["current"] = current

        def add_to_dir_cache(name, size):
            """上传成功一个就立刻写缓存, 中途重启也不会丢记录。"""
            try:
                conn = sqlite3.connect(DB_PATH)
                try:
                    row = conn.execute(
                        "SELECT files_json FROM dir_listing WHERE root=?",
                        (plan["target_root"],)).fetchone()
                    listing = json.loads(row[0]) if row and row[0] else []
                    if any(str(x.get("name")) == name for x in listing):
                        return
                    listing.append({"name": name, "size": size,
                                    "mtime": int(time.time())})
                    conn.execute("""INSERT INTO dir_listing (root,files_json,updated_at)
                        VALUES (?,?,?) ON CONFLICT(root) DO UPDATE SET
                        files_json=excluded.files_json, updated_at=excluded.updated_at""",
                        (plan["target_root"],
                         json.dumps(listing, ensure_ascii=False), time.time()))
                    conn.commit()
                finally:
                    conn.close()
            except Exception as ex:
                log(f"  · 目录缓存即时写入失败 {name}: {ex}")

        def worker():
            result = {"ok": 0, "fail": 0, "results": [],
                      "delete": plan["delete"]}
            try:
                scan_targets = [x["path"] for x in plan["upload"]]
                if scan_targets:
                    progress("扫描元数据", 0, len(scan_targets), "")
                    log(f"上传前本地扫描 {len(scan_targets)} 个")
                    conn = sqlite3.connect(DB_PATH)
                    try:
                        existing = {r[0] for r in conn.execute(
                            "SELECT name FROM archives WHERE has_meta=1 AND error IS NULL")}
                        for si, path in enumerate(scan_targets, 1):
                            name = os.path.basename(path)
                            progress("扫描元数据", si, len(scan_targets), name)
                            if name in existing:
                                log(f"  · {name}: 数据库已有记录, 跳过解析")
                                continue
                            try:
                                rec = scanner.scan_one(path)
                                scanner.save(conn, rec)
                                conn.commit()
                                log(f"  ✓ {name}: 元数据已解析")
                            except Exception as ex:
                                log(f"  ✗ {name}: 元数据解析失败 {ex}")
                    finally:
                        conn.close()

                source = src_layer.get_source(plan["target_root"])
                total_upload = len(plan["upload"])
                progress("上传", 0, total_upload, "")
                for ui, it in enumerate(plan["upload"], 1):
                    name, path = it["name"], it["path"]
                    progress("上传", ui - 1, total_upload, name)
                    rel = name
                    if it.get("action") == "replace_remote" and it.get("remote"):
                        old = it["remote"].get("path") or name
                        if "/" in old:
                            rel = old
                    if it.get("action") == "replace_remote" and it.get("remote"):
                        old_info = it["remote"]
                        old_root = old_info.get("root") or plan["compare_root"]
                        old_rel = old_info.get("name") or name
                        try:
                            old_source = src_layer.get_source(old_root)
                            ret, msg = old_source.delete(old_rel)
                            log(f"  · 删除远程旧文件 {old_root}/{old_rel}: {ret} {msg}")
                        except Exception as ex:
                            log(f"  · 删除远程旧文件失败 {old_root}/{old_rel}: {ex}")
                    log(f"  ↑ 上传 {path} -> {rel}")
                    ret, msg = source.upload(rel, path)
                    if ret == "ok":
                        result["ok"] += 1
                        result["results"].append((path, "ok", msg))
                        add_to_dir_cache(name, it["size"])
                    else:
                        result["fail"] += 1
                        result["results"].append((path, "fail", msg))
                    log(f"  {ret} {path}: {msg}")

                progress("更新缓存", total_upload, total_upload, "")
            except Exception as ex:
                result["error"] = str(ex)[:300]
                log(f"上传失败: {ex}")
            finally:
                progress("完成", UPLOAD_WORKERS[uid]["progress"]["total"],
                         UPLOAD_WORKERS[uid]["progress"]["total"], "")
                UPLOAD_WORKERS[uid]["result"] = result
                UPLOAD_WORKERS[uid]["running"] = False
                write_op_log("上传结束", {"upload_id": uid,
                                      "ok": result["ok"], "fail": result["fail"]})

        threading.Thread(target=worker, daemon=True).start()
        return {"upload_id": uid}

    @staticmethod
    def upload_status(uid):
        w = UPLOAD_WORKERS.get(uid)
        if not w:
            return {"error": "无效 upload_id"}
        return {"running": w["running"], "log": w["log"],
                "progress": w.get("progress"), "result": w["result"]}

    @staticmethod
    def scan_start(dirs, db, rescan, recursive=True):
        sid = f"scan_{int(time.time())}"
        log_dir = os.path.join(BASE, "logs", "scan")
        try:
            os.makedirs(log_dir, exist_ok=True)
        except Exception:
            pass
        log_path = os.path.join(log_dir, sid + ".log")
        try:
            with open(log_path, "w", encoding="utf-8") as f:
                f.write(f"# scan {sid}\n")
                f.write(f"# dirs: {dirs}\n")
                f.write(f"# db: {db}\n")
                f.write(f"# recursive: {recursive}\n")
                f.write(f"# rescan: {rescan}\n")
        except Exception:
            log_path = None
        SCAN_WORKERS[sid] = {"running": True, "stop": False,
                             "log": [], "log_path": log_path, "stats": None,
                             # 进度不是日志行: 界面把它渲染成一行固定文字,
                             # 原地刷新。远程每个文件要一两秒, 930 个文件
                             # 如果每行一条日志会滚成瀑布, 反而看不出进度。
                             "progress": {"done": 0, "total": 0,
                                          "current": "", "phase": "准备"}}
        w = SCAN_WORKERS[sid]

        def log_fn(msg):
            w["log"].append(msg)
            path = w.get("log_path")
            if path:
                try:
                    with open(path, "a", encoding="utf-8") as f:
                        f.write(str(msg) + "\n")
                except Exception:
                    pass

        def progress(done, total, current="", phase=""):
            p = w["progress"]
            p["done"], p["total"] = done, total
            if current:
                p["current"] = current
            if phase:
                p["phase"] = phase

        def worker():
            try:
                stats = scanner.scan_dirs(
                    dirs, db, log=log_fn, rescan=rescan,
                    progress=progress,
                    stop_flag=lambda: w["stop"], recursive=recursive)
                w["stats"] = stats
                path = w.get("log_path")
                if path:
                    try:
                        with open(path, "a", encoding="utf-8") as f:
                            f.write("STATS " + json.dumps(
                                {k: v for k, v in (stats or {}).items()
                                 if k not in ("paths", "names")},
                                ensure_ascii=False) + "\n")
                    except Exception:
                        pass
                write_op_log("扫描结束", {"scan_id": sid, "stats": {
                    k: v for k, v in (stats or {}).items()
                    if k not in ("paths", "names")}})
                # 记下这次扫到的文件名(连"没扫完就停"的那部分也算),
                # 浏览页默认只看这批。一起记下是哪个库 —— 扫描页可以填
                # 别的库, 而浏览页永远读 DB_PATH, 不比对的话会用 A 库的
                # 文件名去过滤 B 库, 结果一片空白。
                if "names" in stats:
                    LAST_SCAN["names"] = stats["names"]
                    LAST_SCAN["db"] = os.path.abspath(db)
                    LAST_SCAN["dirs"] = list(dirs)
                    LAST_SCAN["at"] = time.strftime("%Y-%m-%d %H:%M:%S")
            except Exception as ex:
                w["stats"] = {"error": str(ex)}
            finally:
                w["running"] = False

        t = threading.Thread(target=worker, daemon=True)
        t.start()
        return {"scan_id": sid}

    @staticmethod
    def scan_status(sid):
        w = SCAN_WORKERS.get(sid)
        if not w:
            return {"error": "无效 scan_id"}
        return {"running": w["running"], "log": w["log"],
                "log_path": w.get("log_path"),
                "progress": w.get("progress"), "stats": w["stats"]}

    @staticmethod
    def scan_stop(sid):
        w = SCAN_WORKERS.get(sid)
        if w:
            w["stop"] = True
            return {"ok": True}
        return {"error": "无效 scan_id"}

    @staticmethod
    def scope_names(scope="scan"):
        """浏览页该看哪些文件: None=全库, 列表=本次扫描到的那些。

        scope="all" 看全库。"scan"(默认) 只看本次扫描扫到的 ——
        还没扫过时返回**空列表**, 即"什么都没有"。绝不能返回 None:
        那等于不设范围, 会把整个库倒出来, 跟"只看本次扫描"正好相反
        (重启后 LAST_SCAN 就没了, 一进浏览页就会踩到)。

        若上次扫描用的不是当前库(扫描页可以填别的库), 也当作"没扫过":
        拿别的库的文件名过滤当前库只会得到空结果, 那比退回全库更误导。
        """
        if scope == "all":
            return None
        if LAST_SCAN.get("db") and \
                LAST_SCAN["db"] != os.path.abspath(DB_PATH):
            return []
        return LAST_SCAN["names"] or []

    @staticmethod
    def distinct(field, scope="scan"):
        try:
            names = API.scope_names(scope)
            vals = scanner.distinct_values(DB_PATH, field, names=names)
            # 下拉框按出现次数只取前 500 个。**必须说明截断了** —— 否则
            # 用户在下拉框里找不到某个作者, 会以为库里没有, 其实只是排在后
            # 面没列出来(实测 4958 个作者只列了 500 个)。
            total = API._count_values(field, names)
            return {"values": [{"value": v, "count": c} for v, c in vals],
                    "total": total, "truncated": total > len(vals)}
        except Exception as ex:
            return {"error": str(ex)}

    @staticmethod
    def _reader_rel_path(name):
        """阅读接口接受安全的相对子路径, 拒绝绝对/越界/反斜杠路径。"""
        if not name or not isinstance(name, str):
            return None
        name = name.strip()
        if not name or name.startswith(("/", "\\")) or "\\" in name:
            return None
        parts = name.split("/")
        if any(part in ("", ".", "..") for part in parts):
            return None
        return name

    @staticmethod
    def reader_list(src_dir, conditions, join="and", scope="scan", limit=200,
                    force=False, subdirs=False):
        """阅读首页: 以目录里的文件为准列出所有可阅读压缩包。

        数据库只是元数据缓存: 没入库/没元数据的文件照样显示, 只是字段为空。
        目录列一次就缓存; 只有 force=True(点刷新) 才重新列目录。
        """
        try:
            if not src_dir or not dir_usable(src_dir):
                return {"error": "请填有效的漫画目录(本地路径或 WebDAV 远程路径)"}
            with READER_DIR_LOCK:
                paths = None if force else READER_DIR_CACHE.get(src_dir)
                if paths is not None:
                    paths = dict(paths)
                else:
                    cached = None
                    if not force:
                        try:
                            conn = sqlite3.connect(DB_PATH)
                            row = conn.execute(
                                "SELECT files_json FROM dir_listing WHERE root=?",
                                (src_dir,)).fetchone()
                            conn.close()
                            if row and row[0]:
                                cached = json.loads(row[0])
                        except Exception:
                            cached = None
                    if cached:
                        paths = {}
                        for x in cached:
                            nm = x.get("name")
                            if not nm: continue
                            paths[nm] = {"path": nm, "size": x.get("size") or 0}
                    else:
                        if force:
                            try: src_layer.drop_source(src_dir)
                            except Exception: pass
                        if src_layer.is_remote(src_dir):
                            source = src_layer.get_source(src_dir)
                            paths = {}
                            for rel, name, size, _mtime in source.list_archives(False):
                                paths[name] = {"path": rel, "size": size}
                        else:
                            paths = {}
                            for fn in sorted(os.listdir(src_dir)):
                                full = os.path.join(src_dir, fn)
                                if os.path.isfile(full) and \
                                        os.path.splitext(fn)[1].lower() in scanner.ARCHIVE_EXT:
                                    paths[fn] = {"path": fn, "size": os.path.getsize(full)}
                    READER_DIR_CACHE[src_dir] = paths
            if subdirs and not force:
                # 合并子目录清单: 查所有 root 以当前路径为前缀的缓存行。
                # paths 的 key 是展示名(字符串), 同名文件按文件名去重;
                # size 只是属性, 不参与判断。
                try:
                    conn = sqlite3.connect(DB_PATH)
                    prefix = src_dir.rstrip("/") + "/"
                    rows = conn.execute(
                        "SELECT root,files_json FROM dir_listing WHERE root LIKE ?",
                        (prefix + "%",)).fetchall()
                    conn.close()
                    # 根目录裸文件名优先保留为显示名; 子目录项只要同名,
                    # 就用带子目录的 open path 覆盖, 避免按根目录路径打开 404。
                    for sub_root, fj in rows:
                        for x in json.loads(fj or "[]"):
                            nm = x.get("name")
                            if not nm: continue
                            sz = int(x.get("size") or 0)
                            if src_layer.is_remote(sub_root):
                                sub_rel = sub_root[len(src_dir.rstrip("/")):].lstrip("/")
                            else:
                                sub_rel = os.path.relpath(sub_root, src_dir)
                                if sub_rel == ".": sub_rel = ""
                            if not sub_rel:
                                continue
                            rel = (sub_rel + "/" + nm).lstrip("/")
                            # 不在列表阶段逐文件 HEAD 检查(会把接口拖死)。
                            # 子目录清单已经过扫描/清理, 这里直接用它。
                            paths[nm] = {"path": rel, "size": sz}
                except Exception:
                    pass
            if not paths:
                return {"items": [], "count": 0, "total": 0,
                        "hint": "目录里没有可阅读的漫画压缩包"}
            # 一次性把库里元数据取出来(按名字), 与文件列表做内存关联
            conn = sqlite3.connect(DB_PATH)
            try:
                meta_by_name = {}
                names = list(paths)
                for i in range(0, len(names), 900):
                    chunk = names[i:i+900]
                    ph = ",".join("?" * len(chunk))
                    cols = ["name"] + scanner.META_COLUMNS
                    rows = conn.execute(
                        "SELECT " + ",".join("a." + c if c == "name"
                                            else "m." + c for c in cols) +
                        " FROM archives a LEFT JOIN meta m ON m.archive_id=a.id "
                        f"WHERE a.name IN ({ph})", chunk).fetchall()
                    for r in rows:
                        d = dict(zip(cols, r))
                        # 库里记录的 size 和磁盘文件可能因后补元数据而不同;
                        # 关联以文件名为准, 不拿 size 当 key。
                        meta_by_name[r[0]] = d
            finally:
                conn.close()
            items = []
            for name in sorted(paths, key=str.lower):
                info = paths[name]
                m = meta_by_name.get(name, {})
                it = {"name": name,
                      "open_name": info.get("path") or name,
                      "size": info.get("size") or m.get("size") or 0,
                      "title": m.get("title") or "",
                      "title_original": m.get("title_original") or "",
                      "artist": m.get("artist") or "", "parody": m.get("parody") or "",
                      "tags": m.get("tags") or "", "character": m.get("character") or "",
                      "group_name": m.get("group_name") or "",
                      "category": m.get("category") or "",
                      "language": m.get("language") or "",
                      "year": m.get("year"), "month": m.get("month"),
                      "day": m.get("day"), "page_count": m.get("page_count")}
                items.append(it)
            # 筛选: 有元数据字段按元数据匹配, 标题额外匹配文件名; 没元数据时只匹配文件名
            conds = [c for c in (conditions or [])
                     if (c.get("value") or "").strip()
                     or c.get("op") in ("empty", "not_empty")]
            if conds:
                def match(it, c):
                    f, op, v = c.get("field"), c.get("op"), str(c.get("value") or "")
                    vals = [str(it.get(f) or "")]
                    if f == "title": vals += [str(it.get("title_original") or ""), it["name"]]
                    hit = any(v.lower() in x.lower() for x in vals if x)
                    if op == "contains": return hit
                    if op == "not_contains": return not hit
                    if op == "eq": return any(v.lower() == x.lower() for x in vals if x)
                    if op == "ne": return not any(v.lower() == x.lower() for x in vals if x)
                    if op == "empty": return not any(vals)
                    if op == "not_empty": return any(vals)
                    return True
                items = [it for it in items if (
                    all(match(it,c) for c in conds) if join == "and"
                    else any(match(it,c) for c in conds))]
            total = len(items)
            items = items[:limit]
            return {"items": items, "count": len(items), "total": total,
                    "in_dir": len(paths), "limit": limit}
        except Exception as ex:
            return {"error": str(ex)}

    @staticmethod
    def reader_pages(name, src_dir, size=0):
        """打开漫画时直接读 zip 页列表(自然排序), 不依赖数据库页索引。"""
        started = time.time()
        rel0 = API._reader_rel_path(name)
        write_api_log("READER_PAGES_BEGIN", {"name": rel0, "src_dir": src_dir, "size": size})
        try:
            rel = API._reader_rel_path(name)
            if not src_dir or rel is None:
                write_api_log("READER_PAGES_ERROR", {"reason": "参数错误"})
                return {"error": "参数错误"}
            name = rel
            source = src_layer.get_source(src_dir)
            fp = reader.open_manga(source, name, size=size, root=src_dir)
            if not fp:
                write_api_log("READER_PAGES_ERROR", {"reason": "找不到这个文件", "name": name})
                return {"error": "找不到这个文件"}
            pages = reader._list_pages_fp(fp)
            if not pages:
                write_api_log("READER_PAGES_ERROR", {"reason": "没有图片", "name": name})
                return {"error": "不是可阅读的 zip, 或里面没有图片"}
            write_api_log("READER_PAGES_OK", {"name": name, "count": len(pages),
                                              "first": pages[0],
                                              "elapsed": round(time.time()-started, 3)})
            return {"name": name, "pages": pages, "count": len(pages)}
        except Exception as ex:
            write_api_log("READER_PAGES_EXCEPTION", {"error": str(ex)})
            return {"error": str(ex)}

    @staticmethod
    def library_candidates(src_dir, q="", limit=40):
        """已入库漫画的文件名候选, 供阅读页搜索文本框用。"""
        try:
            if not src_dir or not dir_usable(src_dir):
                return {"values": []}
            source = (src_layer.get_source(src_dir)
                      if src_layer.is_remote(src_dir) else None)
            found = fileops.match_in_db(DB_PATH, src_dir, False, source=source)
            names = [i["name"] for i in found]
            if q:
                q = q.lower()
                names = [n for n in names if q in n.lower()]
            return {"values": names[:limit]}
        except Exception:
            return {"values": []}

    @staticmethod
    def reader_thumb(name, size, src_dir):
        if not src_dir:
            write_api_log("READER_THUMB_SKIP", {"reason": "没有 src_dir", "name": name})
            return None
        try:
            size = int(size)
        except (TypeError, ValueError):
            write_api_log("READER_THUMB_SKIP", {"reason": "size 非数字", "name": name, "size": size})
            return None
        # 只接受文件名, 不接受带斜杠/..的路径
        name = API._reader_rel_path(name)
        if not src_dir or name is None:
            write_api_log("READER_THUMB_SKIP", {"reason": "非法路径", "name": name})
            return None
        # 缓存命中: reader.get_thumb 直接查本地缓存, 不碰数据源;
        # 未命中才让数据源(本地/WebDAV)读 zip 第一张图。
        try:
            source = src_layer.get_source(src_dir)
        except Exception as ex:
            write_api_log("READER_THUMB_SOURCE_ERROR", {"name": name, "error": str(ex)})
            return None
        started=time.time()
        got=reader.get_thumb(name, size, source=source, root=src_dir)
        write_api_log("READER_THUMB_RESULT", {"name": name, "ok": bool(got),
                                              "mime": got[1] if got else None,
                                              "elapsed": round(time.time()-started, 3)})
        return got

    @staticmethod
    def reader_page_image(name, src_dir, entry, size=0):
        rel = API._reader_rel_path(name)
        if not src_dir or rel is None:
            write_api_log("READER_PAGE_SKIP", {"reason": "参数错误", "entry": entry})
            return None
        name = rel
        started=time.time()
        try:
            source = src_layer.get_source(src_dir)
            fp = reader.open_manga(source, name, size=size, root=src_dir)
            got = reader.get_page_fp(fp, entry) if fp else None
            write_api_log("READER_PAGE_RESULT", {"name": name, "entry": entry,
                                                 "ok": bool(got),
                                                 "bytes": len(got[0]) if got else 0,
                                                 "elapsed": round(time.time()-started, 3)})
            return got
        except Exception as ex:
            write_api_log("READER_PAGE_EXCEPTION", {"name": name, "entry": entry,
                                                    "error": str(ex)})
            return None

    @staticmethod
    def _count_values(field, names):
        """某字段在当前范围内的不同值总数(不受下拉框 500 上限影响)。"""
        conn = sqlite3.connect(DB_PATH)
        try:
            scope = scanner.scope_clause(conn, names)
            return conn.execute(
                "SELECT COUNT(DISTINCT mv.value) FROM meta_values mv "
                "JOIN archives a ON a.id=mv.archive_id "
                f"WHERE mv.field=? AND mv.value IS NOT NULL "
                f"AND mv.value != '' AND ({scope})", (field,)).fetchone()[0]
        finally:
            conn.close()

    @staticmethod
    def _count(where, args, names):
        """符合条件的作品总数(不受 limit 影响)。用于判断结果是否被截断。"""
        conn = sqlite3.connect(DB_PATH)
        try:
            scope = scanner.scope_clause(conn, names)
            return conn.execute(
                "SELECT COUNT(*) FROM archives a "
                f"WHERE ({where}) AND ({scope})", args).fetchone()[0]
        finally:
            conn.close()

    @staticmethod
    def query(field, value, contains, limit=2000, scope="scan"):
        # 用 meta_values 的 EXISTS 而不是 meta 的原始列:
        # 原始列是多值逗号串, 等值匹配不上(整串不等于单值);
        # 而且它没经过"视为空"的过滤, 会查出分类时根本不会出现的值
        # (如原作里的 original)。
        where, args = "1=1", []
        if contains and field:
            where = ("EXISTS (SELECT 1 FROM meta_values mv WHERE "
                     "mv.archive_id=a.id AND mv.field=? AND mv.value LIKE ?)")
            args = [field, f"%{contains}%"]
        elif value and field:
            where = ("EXISTS (SELECT 1 FROM meta_values mv WHERE "
                     "mv.archive_id=a.id AND mv.field=? AND mv.value=?)")
            args = [field, value]
        try:
            names = API.scope_names(scope)
            # 先数总数, 再取这一页 —— 单看 count 分不清"就这么多"和
            # "被 limit 截断了"。截断却不提示, 用户会以为这就是全部,
            # 照着它去判断/分类就漏了。
            total = API._count(where, args, names)
            cols, rows = scanner.list_archives(DB_PATH, where, args, limit,
                                               names=names)
            # 多值字段的条目数, 供前端显示 [N] 徽标。
            # 只统计这一页涉及的作品, 不整表扫 —— meta_values 有 120 多万行,
            # 每次查询都全表 GROUP BY 会白白多花几百毫秒。
            page_ids = [dict(zip(cols, r)).get("id") for r in rows]
            conn = sqlite3.connect(DB_PATH)
            try:
                cnt = {}
                if page_ids:
                    ph = ",".join("?" * len(page_ids))
                    for aid, f, n in conn.execute(
                            "SELECT archive_id, field, COUNT(*) FROM meta_values "
                            f"WHERE archive_id IN ({ph}) "
                            "GROUP BY archive_id, field", page_ids):
                        cnt.setdefault(aid, {})[f] = n
                name_to_id = dict(conn.execute("SELECT name, id FROM archives"))
            finally:
                conn.close()
            counts = []
            for r in rows:
                d = dict(zip(cols, r))
                counts.append(cnt.get(name_to_id.get(d.get("name")), {}))
            return {"columns": cols, "rows": rows, "count": len(rows),
                    "total": total, "truncated": total > len(rows),
                    "limit": limit, "value_counts": counts}
        except Exception as ex:
            return {"error": str(ex)}

    @staticmethod
    def detail(name):
        try:
            cols, rows = scanner.list_archives(DB_PATH, "a.name = ?", (name,))
            if not rows:
                return {"error": "未找到"}
            d = dict(zip(cols, rows[0]))
            # 把 other_fields 解析出来
            of = d.get("other_fields")
            if of:
                try:
                    d["other_fields"] = json.loads(of)
                except Exception:
                    pass
            return d
        except Exception as ex:
            return {"error": str(ex)}

    @staticmethod
    def source_list(src_dir, recursive):
        """列出源目录下已入库的压缩包(库里没有的忽略)。支持远程源。"""
        try:
            source = (src_layer.get_source(src_dir)
                      if src_layer.is_remote(src_dir) else None)
            items = fileops.match_in_db(DB_PATH, src_dir, recursive,
                                        source=source)
            return {"items": items, "count": len(items),
                    "remote": remote_note(src_dir)}
        except Exception as ex:
            return {"error": str(ex)}

    @staticmethod
    def filter_preview(conditions, join, base_dir, group_by, src_dir=None,
                       recursive=False, limit=5000, group_dir=False):
        """按条件算出条目 + 目标路径,不碰文件。

        条目级: 一个作品的多值字段会展开成多个条目, 每个条目独立判断,
        筛选后剩下的值直接决定分类目录。

        库不记路径, 所以源目录是"文件在哪"的唯一来源, 必填。
        """
        try:
            if not src_dir:
                return {"error": "请先填源目录 —— 库不记文件路径,"
                                 "要知道文件在哪才能算出目标"}
            if not dir_usable(src_dir):
                return {"error": f"源目录不存在: {src_dir}"}
            # 目标与源必须在同一存储上(都本地, 或同一个 WebDAV remote)。
            # 跨存储只能是下载再上传, 本次不做 —— 与其悄悄变慢, 不如直说。
            if not src_layer.same_store(src_dir, base_dir):
                return {"error": "源目录与目标目录必须在同一存储上 "
                                 "(都为本地, 或同一个 WebDAV remote); "
                                 "跨存储搬运需要下载再上传, 暂不支持"}
            # 先拿到源目录里都有哪些文件, 再让查询**在 SQL 层**按这个
            # 名单收窄。顺序不能反: 查询有 limit(按 name 排序取前 N),
            # 先查全库再过滤的话, 名字排在后面的目标会被 limit 截掉,
            # 界面就报"没有可分类的"。
            source = (src_layer.get_source(src_dir)
                      if src_layer.is_remote(src_dir) else None)
            found = fileops.match_in_db(DB_PATH, src_dir, recursive,
                                        source=source)
            paths = {i["name"]: i["src"] for i in found}
            # limit 按作品计。范围已经收窄到源目录, 所以上限就取源目录里
            # 的文件数 —— 用默认 5000 的话, 源目录超过 5000 个文件时会
            # 静默漏掉一部分, 用户还以为都处理了。
            entries = scanner.query_entries(DB_PATH, conditions, join,
                                            fields=[group_by],
                                            limit=max(limit, len(paths) or 1),
                                            names=list(paths))
            # 按作品聚合成 {name: [值...]}
            by_name = {}
            for e in entries:
                by_name.setdefault(e["name"], [])
                if e["value"] not in by_name[e["name"]]:
                    by_name[e["name"]].append(e["value"])
            # 在该字段上没有任何值的作品(如原作只有 original 的原创作品):
            # 记成空列表, 让它们落到「未分类」。
            # 不补进来的话它们根本没有条目, 会整条被丢掉 —— 永远分类不到。
            fallback = []
            if group_by:
                fallback = scanner.no_value_names(
                    DB_PATH, conditions, join, field=group_by,
                    names=list(paths))
                for n in fallback:
                    by_name.setdefault(n, [])
            names = list(by_name)
            # paths 只包含源目录里真实存在的文件; 源目录没填时为空 ->
            # exists_src 全 False, 预览会提示"找不到", 这是诚实的。
            # 远程也用真实判断(查后端清单), 不能用恒 True 的回调 ——
            # 那会让"文件已经被搬走"在预览里看不出来。
            remote = src_layer.is_remote(src_dir)
            if remote:
                def _exists(p):
                    return source.exists(fileops._rel_of(p, source.root))
            else:
                _exists = os.path.isfile
            plan = fileops.plan_transfer(
                DB_PATH, names, base_dir, group_by,
                entries=by_name, group_dir=group_dir, paths=paths,
                fallback_names=fallback, exists=_exists)
            # 预览按条目展开: 一个作品多个值就是多行
            rows = []
            for p in plan:
                for t in p["targets"]:
                    rows.append({"name": p["name"], "src": p["src"],
                                 "dest_dir": t,
                                 "dest": src_layer.join(t, p["name"]),
                                 "targets": p["targets"],
                                 "exists_src": p["exists_src"]})
            # 扇出上限丢值的作品: 必须说出来, 不能让用户以为都归档了
            truncated = [{"name": p["name"], "dropped": p["dropped"]}
                         for p in plan if p.get("dropped")]
            # 不同值清理后同名、会并进同一目录的作品
            merged = [{"name": p["name"], "merged": p["merged"]}
                      for p in plan if p.get("merged")]
            return {"plan": plan, "rows": rows, "count": len(plan),
                    "entry_count": len(rows),
                    "truncated": truncated, "merged": merged,
                    "remote": remote_note(src_dir),
                    "copy_unsupported": bool(remote),
                    "missing": [p["name"] for p in plan
                                if not p.get("exists_src")]}
        except Exception as ex:
            return {"error": str(ex)}

    @staticmethod
    def classify_go(approved, dest, mode, group_by="", src_dir=None,
                    recursive=False, conditions=None, join="and",
                    group_dir=False):
        """approved: 用户在预览里确认过的那批文件名(必传)。

        移动不可逆, 所以把它当作**上界**: 实际只处理"条件筛出来的"与
        "用户确认过的"交集。用户在预览后又改了条件时, 服务端重新推导的
        集合会比确认框里说的大 —— 那就可能搬走没给他看过、没经他确认
        的文件。取交集保证只会更少, 不会更多。
        """
        cid = f"cls_{int(time.time())}"
        w = {"running": True, "stop": False, "log": [], "result": None}
        CLS_WORKERS[cid] = w

        def log_fn(msg):
            w["log"].append(msg)

        def progress(i, total):
            pass

        def worker():
            try:
                # 源目录必填: 库不记路径, 没有它就不知道文件在哪。
                # 移动不可逆, 不能让它带着空名单一路跑成"全部失败"。
                if not src_dir:
                    w["result"] = {"error": "请先填源目录 —— 库不记文件路径,"
                                            "要知道文件在哪才能搬"}
                    return
                if not dir_usable(src_dir):
                    w["result"] = {"error": f"源目录不存在: {src_dir}"}
                    return
                # 目标与源必须同一存储(都本地, 或同一个 WebDAV remote)
                if not src_layer.same_store(src_dir, dest):
                    w["result"] = {
                        "error": "源目录与目标目录必须在同一存储上 "
                                 "(都为本地, 或同一个 WebDAV remote); "
                                 "跨存储搬运需要下载再上传, 暂不支持"}
                    return
                remote = src_layer.is_remote(src_dir)
                source = src_layer.get_source(src_dir) if remote else None
                paths = {i["name"]: i["src"] for i in
                         fileops.match_in_db(DB_PATH, src_dir, recursive,
                                             source=source)}
                # 同样在 SQL 层按源目录收窄; limit 取源目录文件数,
                # 免得超过默认上限时静默漏掉一部分文件
                entries = scanner.query_entries(
                    DB_PATH, conditions or [], join, fields=[group_by],
                    limit=max(len(paths), 1), names=list(paths))
                by_name = {}
                for e in entries:
                    by_name.setdefault(e["name"], [])
                    if e["value"] not in by_name[e["name"]]:
                        by_name[e["name"]].append(e["value"])
                # 该字段没值的作品归「未分类」, 不能整条丢掉
                fallback = []
                if group_by:
                    fallback = scanner.no_value_names(
                        DB_PATH, conditions or [], join, field=group_by,
                        names=list(paths))
                    for n in fallback:
                        by_name.setdefault(n, [])
                use_names = list(by_name)
                # approved 是**上界**, 且"空列表"要当成"什么都没确认",
                # 不能当成"不设限"。用 `if approved:` 判的话, 空名单会让
                # 收窄整段跳过 -> 条件命中的文件**全部**被搬走 —— 移动
                # 不可逆, 这正是最该防的: 预览里一个都没确认, 结果全搬了。
                if approved is not None:
                    allowed = set(approved)
                    dropped = [n for n in use_names if n not in allowed]
                    use_names = [n for n in use_names if n in allowed]
                    if dropped:
                        log_fn(f"⚠ {len(dropped)} 个文件不在预览确认的范围内, "
                               "已排除(预览后条件被改过?)")
                    by_name = {n: v for n, v in by_name.items() if n in allowed}
                    # fallback 也要一起收窄, 否则会被重新塞回 by_name
                    fallback = [n for n in fallback if n in allowed]
                if not use_names:
                    log_fn("筛选后没有任何条目, 无事可做")
                result, errs = fileops.transfer_by_names(
                    DB_PATH, use_names, dest, mode,
                    log=log_fn, progress=progress,
                    stop_flag=lambda: w["stop"], group_by=group_by,
                    entries=by_name, group_dir=group_dir, paths=paths,
                    fallback_names=fallback, source=source)
                w["result"] = result
                w["errors"] = errs
            except Exception as ex:
                w["result"] = {"error": str(ex)}
            finally:
                # 不管成功失败都要丢掉清单缓存: 失败时可能已经搬走了一部分,
                # 留着旧清单会让下次预览显示错误的位置。
                if src_layer.is_remote(src_dir or ""):
                    src_layer.drop_source(src_dir)
                w["running"] = False

        t = threading.Thread(target=worker, daemon=True)
        t.start()
        return {"cls_id": cid}

    @staticmethod
    def classify_status(cid):
        w = CLS_WORKERS.get(cid)
        if not w:
            return {"error": "无效 cls_id"}
        return {"running": w["running"], "log": w["log"],
                "result": w.get("result"), "errors": w.get("errors")}

    @staticmethod
    def classify_stop(cid):
        w = CLS_WORKERS.get(cid)
        if w:
            w["stop"] = True
            return {"ok": True}
        return {"error": "无效 cls_id"}

    @staticmethod
    def stats(scope="scan"):
        """总览: 各字段的基数概览(不含明细,明细走 /api/stat_field)。

        scope 默认 "scan": 只统计**本次扫描到的**那些文件, 不是整库 ——
        库里还堆着以前扫过的作品, 把它们算进来会让"这次扫完效果如何"
        永远看不出来。"all" 才是全库。
        """
        try:
            names = API.scope_names(scope)
            conn = sqlite3.connect(DB_PATH)
            try:
                scope_sql = scanner.scope_clause(conn, names)
                total = conn.execute(
                    f"SELECT COUNT(*) FROM archives a WHERE ({scope_sql})"
                    ).fetchone()[0]
                with_meta = conn.execute(
                    f"SELECT COUNT(*) FROM archives a "
                    f"WHERE a.has_meta=1 AND ({scope_sql})").fetchone()[0]
                errors = conn.execute(
                    f"SELECT COUNT(*) FROM archives a "
                    f"WHERE a.error IS NOT NULL AND ({scope_sql})"
                    ).fetchone()[0]
                # 各字段: 不同值个数 + 条目总数, 供字段选择器显示规模。
                # 只统计有分类意义的字段(见 scanner.STAT_FIELDS)
                # 用 archive_id IN (子查询) 而不是 JOIN archives: JOIN 会让
                # 优化器顺着 idx_mv_field_value(field,value) 先扫该字段的
                # 全部行(全库 120 万), 2000 个文件的范围实测要 52 秒;
                # IN 子查询则先收窄出范围内的 id, 再按 idx_mv_archive 取。
                # 所有字段一次 GROUP BY 拿全: 逐字段各查一次要 8 遍扫描,
                # 实测 2.7 秒, 合成一句 0.08 秒。
                in_scope = (f"mv.archive_id IN (SELECT a.id FROM archives a "
                            f"WHERE {scope_sql})")
                counts = {
                    f: (n, m) for f, n, m in conn.execute(
                        "SELECT mv.field, COUNT(DISTINCT mv.value), COUNT(*) "
                        "FROM meta_values mv "
                        f"WHERE {in_scope} GROUP BY mv.field")}
                fields = []
                for f in scanner.STAT_FIELDS:
                    n, m = counts.get(f, (0, 0))
                    if n:
                        fields.append({"field": f, "distinct": n,
                                       "entries": m})
                fields.sort(key=lambda x: -x["distinct"])
                avg_tags = 0
                if total:
                    avg_tags = round(counts.get("tags", (0, 0))[1] / total, 1)
            finally:
                conn.close()
            return {"total": total, "with_meta": with_meta,
                    "errors": errors, "fields": fields,
                    "avg_tags": avg_tags}
        except Exception as ex:
            return {"error": str(ex)}

    @staticmethod
    def stat_field(field, limit=20, offset=0, q="", sort="count",
                   scope="scan"):
        """某字段的分布明细, 分页 + 搜索。

        scope:        "scan"(默认) 只算本次扫描到的 | "all" 全库
        limit/offset: 分页, 避免一次拉几万行
        q:            过滤值(子串匹配)
        sort:         count(按作品数) | name(按名称)
        返回 top(当前页) + total_values(该字段不同值总数) +
             tail(剩余值的数量和作品数, 用于显示"其他 N 项")
        """
        if field not in scanner.STAT_FIELDS:
            return {"error": f"非法字段: {field}"}
        try:
            names = API.scope_names(scope)
            conn = sqlite3.connect(DB_PATH)
            try:
                scope_sql = scanner.scope_clause(conn, names)
                # 范围条件里用到了 archives(a.name), 所以必须收窄 archive_id。
                # 走 IN 子查询而不是 JOIN: JOIN 时优化器会先按
                # idx_mv_field_value(field,value) 扫该字段全部行(全库 120 万),
                # 范围小的时候反而慢上百倍。
                src = ("FROM meta_values mv "
                       "WHERE mv.field=? AND "
                       f"mv.archive_id IN (SELECT a.id FROM archives a "
                       f"WHERE {scope_sql})")
                args = [field]
                if q:
                    src += " AND mv.value LIKE ?"
                    args.append(f"%{q}%")
                total_values = conn.execute(
                    f"SELECT COUNT(DISTINCT mv.value) {src}", args
                    ).fetchone()[0]
                order = ("mv.value ASC" if sort == "name"
                         else "c DESC, mv.value ASC")
                rows = conn.execute(
                    f"SELECT mv.value, COUNT(DISTINCT mv.archive_id) c "
                    f"{src} GROUP BY mv.value "
                    f"ORDER BY {order} LIMIT ? OFFSET ?",
                    args + [limit, offset]).fetchall()
                # 本级之外还剩多少(长尾折叠)
                tail = conn.execute(
                    f"SELECT COUNT(*), COALESCE(SUM(c),0) FROM ("
                    f"  SELECT COUNT(DISTINCT mv.archive_id) c {src} "
                    f"  GROUP BY mv.value "
                    f"  ORDER BY {order} LIMIT -1 OFFSET ?)",
                    args + [offset + limit]).fetchone()
            finally:
                conn.close()
            return {"field": field,
                    "top": [{"name": v, "count": c} for v, c in rows],
                    "total_values": total_values,
                    "shown": offset + len(rows),
                    "tail_values": tail[0] if tail else 0,
                    "tail_entries": tail[1] if tail else 0}
        except Exception as ex:
            return {"error": str(ex)}

    @staticmethod
    def plugins():
        plugs = plugin_loader.load_plugins(os.path.join(BASE, "plugins"))
        return {"plugins": [{"name": getattr(p, "NAME", p.__name__),
                             "file": p.__file__}
                            for p in plugs]}


# ================================================================ HTTP

HTML_PAGE = r"""<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>漫画管理</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,system-ui,sans-serif;background:#f5f5f5;color:#333}
.nav{position:fixed;bottom:0;left:0;right:0;background:#fff;border-top:1px solid #ddd;display:flex;z-index:100}
.nav a{flex:1;text-align:center;padding:10px 2px;text-decoration:none;color:#666;font-size:12px}
.nav a.active{color:#1976d2;font-weight:bold}
.nav a .icon{font-size:20px;display:block;margin-bottom:2px}
.content{padding:12px;padding-bottom:70px;max-width:900px;margin:0 auto}
h2{font-size:18px;margin-bottom:10px;color:#1976d2}
.card{background:#fff;border-radius:8px;padding:14px;margin-bottom:10px;box-shadow:0 1px 3px rgba(0,0,0,.1)}
input,select,button{font-size:14px;padding:8px;border-radius:6px;border:1px solid #ddd}
input[type=text]{width:100%}
button{cursor:pointer;background:#1976d2;color:#fff;border:none;padding:8px 16px}
button:disabled{opacity:.5}
button.outline{background:#fff;color:#1976d2;border:1px solid #1976d2}
.row{display:flex;gap:6px;align-items:center;flex-wrap:wrap;margin-bottom:8px}
.row>input,.row>select{flex:1;min-width:120px}
table{width:100%;border-collapse:collapse;font-size:12px}
th,td{text-align:left;padding:6px 8px;border-bottom:1px solid #eee}
th{background:#f0f0f0;position:sticky;top:0}
tr{cursor:pointer}
tr:hover{background:#e3f2fd}
.log{background:#1e1e1e;color:#d4d4d4;font-family:monospace;font-size:12px;
  padding:10px;border-radius:6px;white-space:pre-wrap;max-height:300px;overflow-y:auto}
.detail-pair{display:flex;padding:6px 0;border-bottom:1px solid #f0f0f0}
.detail-key{width:100px;font-weight:bold;color:#666;flex-shrink:0}
.detail-val{flex:1;word-break:break-all}
.tag{display:inline-block;background:#e3f2fd;color:#1565c0;border-radius:12px;
  padding:2px 8px;margin:2px;font-size:12px}
.bar{height:24px;background:#e0e0e0;border-radius:4px;overflow:hidden;margin:2px 0}
.bar-fill{height:100%;background:#1976d2;color:#fff;font-size:11px;
  line-height:24px;padding-left:6px;white-space:nowrap}
checkbox-row{display:block;margin:4px 0}
.hidden{display:none}
.progress{width:100%;height:6px;background:#e0e0e0;border-radius:3px;margin:6px 0}
.progress-fill{height:100%;background:#4caf50;border-radius:3px;transition:width .3s}
.badge{display:inline-block;background:#ff9800;color:#fff;border-radius:10px;
  padding:1px 7px;font-size:11px;margin-left:4px}
.cond{display:flex;gap:6px;align-items:center;margin-bottom:6px;flex-wrap:wrap}
.cond select,.cond input{flex:1;min-width:80px;font-size:13px;padding:6px}
.cond .del{background:#ef5350;padding:6px 10px;font-size:13px;flex:0 0 auto}
.cond-idx{font-size:11px;color:#999;width:24px;flex:0 0 auto;text-align:center}
/* 值输入框 + 历史按钮: 输入框占满, 按钮挂在右边缘, 不额外占宽度 */
.c-val-wrap{position:relative;flex:1;min-width:80px;display:flex}
.c-val-wrap input{width:100%;padding-right:30px}
.c-val-wrap .hist-btn{position:absolute;right:1px;top:1px;bottom:1px;width:26px;
  padding:0;font-size:13px;line-height:1;background:transparent;color:#999;
  border:none;border-left:1px solid #eee;border-radius:0 5px 5px 0}
.c-val-wrap .hist-btn:hover{background:#f0f0f0;color:#1976d2}
.hist-pop{position:absolute;right:0;top:calc(100% + 2px);z-index:150;
  background:#fff;border:1px solid #ddd;border-radius:6px;min-width:160px;
  max-width:calc(100vw - 32px);max-height:190px;overflow-y:auto;
  box-shadow:0 4px 12px rgba(0,0,0,.15);padding:4px 0}
.hist-pop .hint{font-size:11px;color:#999;padding:4px 10px;border-bottom:1px solid #f0f0f0}
.hist-pop .hi{font-size:13px;padding:7px 10px;cursor:pointer;white-space:nowrap;
  overflow:hidden;text-overflow:ellipsis}
.hist-pop .hi:hover{background:#e3f2fd}
.hist-pop .hi .hi-x{float:right;color:#bbb;margin-left:8px}
.hist-pop .hi .hi-x:hover{color:#ef5350}
.rd-hist-pop{position:absolute;z-index:160;left:12px;right:12px;margin-top:2px;
  background:#fff;border:1px solid #ddd;border-radius:6px;max-height:240px;
  overflow-y:auto;box-shadow:0 4px 12px rgba(0,0,0,.15);padding:4px 0}
.rd-hist-pop .rd-hi{font-size:13px;padding:8px 10px;cursor:pointer;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis;border-bottom:1px solid #f5f5f5}
.rd-hist-pop .rd-hi:active{background:#e3f2fd}
.dest-path{font-size:11px;color:#2e7d32;word-break:break-all}
/* 文件名完整显示不截断, 长名字靠换行容纳 */
#cls-table td:first-child{word-break:break-all}
.hl{background:#fff9c4;padding:0 2px;border-radius:2px}
.badge.mv{cursor:pointer;background:#7e57c2}
.vlist{margin-top:4px;line-height:1.8}
.vlist .tag{font-size:11px}

/* 右上角设置悬浮窗 */
#settings-fab{width:34px;height:34px;flex:0 0 34px;
  border-radius:50%;border:1px solid #ddd;background:#fff;color:#555;
  font-size:18px;line-height:32px;text-align:center;cursor:pointer}
#settings-modal{position:fixed;inset:0;background:rgba(0,0,0,.45);
  z-index:310;display:none;align-items:center;justify-content:center}
#settings-modal.show{display:flex}
#settings-modal>.card{width:88%;max-width:420px;background:#fff;border-radius:8px;
  padding:16px;max-height:80vh;overflow-y:auto}

/* 详情弹窗: 默认隐藏(#detail-modal 没有 .show 时不显示) */
#detail-modal,#dup-modal{position:fixed;inset:0;background:rgba(0,0,0,.5);
  z-index:200;display:none;align-items:center;justify-content:center}
#detail-modal.show,#dup-modal.show{display:flex}
#detail-modal>.card,#dup-modal>.card{width:90%;max-width:500px;max-height:80vh;overflow-y:auto}
.md-chip{display:inline-block;padding:4px 9px;margin:2px;border-radius:14px;
  border:1px solid #ddd;background:#fff;color:#555;font-size:12px;cursor:pointer;
  -webkit-tap-highlight-color:transparent}
.md-chip.on{background:#1976d2;color:#fff;border-color:#1976d2}
.md-group{margin:6px 0 10px;border-bottom:1px solid #f0f0f0;padding-bottom:8px}
.md-group b{font-size:12px;color:#888;display:block;margin-bottom:4px}

/* 阅读首页 */
.rd-fav{display:flex;flex-wrap:wrap;gap:6px;margin:6px 0}
.rd-chip{display:inline-flex;align-items:center;gap:2px;font-size:13px;
  padding:3px 3px 3px 10px;border-radius:16px;border:1px solid #ddd;
  background:#fff;color:#555;cursor:pointer;white-space:nowrap}
.rd-chip.on{background:#1976d2;color:#fff;border-color:#1976d2}
.rd-chip .rd-label{cursor:pointer}
.rd-chip .rd-del{display:inline-flex;align-items:center;justify-content:center;
  width:28px;height:28px;margin-left:2px;border-radius:50%;
  font-size:17px;line-height:1;color:#999;background:transparent;
  border:none;padding:0;cursor:pointer;touch-action:manipulation}
.rd-chip .rd-del:hover,.rd-chip .rd-del:active{background:#ffebee;color:#d32f2f}
.rd-chip .rd-del.confirm{width:auto;border-radius:14px;padding:0 9px;
  background:#d32f2f;color:#fff;font-size:12px;font-weight:bold}
.rd-chip .rd-del.confirm:hover,.rd-chip .rd-del.confirm:active{background:#b71c1c;color:#fff}
.rd-chip.on .rd-del{color:#fff}
.rd-chip.on .rd-del:hover,.rd-chip.on .rd-del:active{background:#fff;color:#d32f2f}
.rd-chip.fav::before{content:'★';color:#f9a825;margin-right:4px}
.rd-chip.on.fav::before{color:#fff}
.rd-grid{display:grid;gap:2px;grid-template-columns:repeat(2,1fr);margin:0 -12px;background:#000}
.rd-card{background:#fff;border-radius:0;overflow:hidden;box-shadow:none;
  cursor:pointer}
.rd-thumb{width:100%;aspect-ratio:3/4;background:#eee;object-fit:cover;display:block}
.rd-meta{padding:3px 5px;background:#fff}
.rd-meta-btn{cursor:pointer}
.rd-meta-btn:active{background:#e3f2fd}
.rd-name{font-size:12px;line-height:1.35;word-break:break-all;
  display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}
.rd-sub{font-size:11px;color:#888;margin-top:2px;white-space:nowrap;
  overflow:hidden;text-overflow:ellipsis}
.rd-meta-row{display:flex;justify-content:space-between;gap:6px;font-size:11px;
  color:#888;margin-top:2px;white-space:nowrap;overflow:hidden}
.rd-meta-row span{overflow:hidden;text-overflow:ellipsis}
.rd-meta-row .rd-date{flex:0 0 auto;color:#aaa}
.rd-loading{font-size:12px;color:#999;text-align:center;padding:12px}
/* 分组视图: 左侧竖排组名, 右侧横滑缩略图 */
.rd-grow{display:flex;align-items:stretch;border-bottom:1px solid #ddd;min-height:148px}
.rd-glabel{width:22px;flex:0 0 22px;writing-mode:vertical-rl;text-orientation:mixed;
  display:flex;align-items:center;justify-content:center;font-size:11px;
  background:#f5f5f5;color:#555;word-break:break-all;padding:6px 0}
.rd-gstrip{flex:1;display:flex;gap:6px;overflow-x:auto;padding:6px;
  -webkit-overflow-scrolling:touch;min-width:0}
.rd-gcard{flex:0 0 104px;width:104px;cursor:pointer}
.rd-gcard img{width:104px;height:140px;object-fit:cover;background:#eee;
  border-radius:3px;display:block}
.rd-gname{font-size:12px;line-height:1.35;color:#555;margin-top:3px;
  word-break:break-all;display:-webkit-box;-webkit-line-clamp:2;
  -webkit-box-orient:vertical;overflow:hidden}
.rd-gmore{flex:0 0 44px;width:44px;border:none;border-left:1px solid #eee;
  background:#fafafa;font-size:12px;color:#1976d2;cursor:pointer;padding:0}
.rd-gexpand{display:grid;grid-template-columns:repeat(4,1fr);gap:4px;
  padding:6px;background:#fafafa}
.rd-gexpand .rd-thumb{aspect-ratio:3/4}
/* 阅读器: 全屏翻页 */
html.rd-open,body.rd-open{height:100%;overflow:hidden;overscroll-behavior:none}
body.rd-open .nav{display:none}
#reader-modal{position:fixed;inset:0;background:#111;z-index:1000;display:none;
  flex-direction:column;overscroll-behavior:contain}
#reader-modal.show{display:flex}
.rd-top{display:flex;align-items:center;gap:8px;padding:8px 10px;color:#fff;
  background:rgba(0,0,0,.7);font-size:13px;flex:0 0 auto}
.rd-top .rd-title{flex:1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.rd-top button{padding:5px 10px;font-size:13px;background:#333;color:#fff;
  border-radius:5px;white-space:nowrap}
.rd-top button.mode{background:#1565c0}
.rd-stage{flex:1;min-height:0;overflow:hidden;display:flex;align-items:center;
  justify-content:center;-webkit-overflow-scrolling:touch;overscroll-behavior:contain}
.rd-stage.scroll-mode{display:block;overflow-y:auto;overscroll-behavior:contain;
  touch-action:pan-y}
.rd-stage img{width:100%;max-width:900px;display:block}
</style>
</head>
<body>

<div id="settings-modal" onclick="if(event.target===this)toggleSettings()">
  <div class="card">
    <div class="row" style="justify-content:space-between;margin-bottom:10px">
      <b>设置</b><button class="outline" onclick="toggleSettings()">关闭</button>
    </div>
    <div style="font-size:13px;line-height:2">
      <div><b>远程漫画缓存</b></div>
      <div>缓存上限(MB):
        <input type="number" id="rd-cache-max" min="50" max="5000" step="50"
               style="width:90px;padding:5px" value="500">
      </div>
      <div id="rd-cache-stats" style="color:#666;font-size:12px"></div>
      <div class="row" style="gap:8px;margin-top:8px">
        <button class="outline" onclick="rdSaveCacheMax()">保存上限</button>
        <button class="outline" onclick="rdClearCache()">清空缓存</button>
      </div>
      <p style="font-size:12px;color:#888">缓存远程漫画整包；超出上限时自动清理最久未打开的。</p>
    </div>
  </div>
</div>

<div class="content" id="app">

<!-- ===== 阅读首页 ===== -->
<div id="page-read" class="page">
<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px">
  <h2 style="margin:0">📖 阅读</h2>
  <button id="settings-fab" onclick="toggleSettings()" title="设置">⚙</button>
</div>
<div class="card">
  <div class="row">
    <input type="text" id="rd-dir" value="/storage/emulated/0/Download">
    <button class="outline" onclick="dirHistToggle('rd')" style="flex:0 0 auto;padding:6px 8px">历史</button>
  </div>
  <div id="rd-dir-hist-box" class="rd-hist-pop" style="display:none"></div>
  <div class="row" style="font-size:13px;align-items:center">
    <label><input type="checkbox" id="rd-subdirs" onchange="rdSaveOpts();rdLoad()"> 显示子文件夹</label>
  </div>
  <div class="row">
    <label style="font-size:13px">每行
      <select id="rd-cols" onchange="rdApplyCols()" style="padding:4px">
        <option value="2">2 本</option>
        <option value="3">3 本</option>
        <option value="4">4 本</option>
      </select>
    </label>
    <label style="font-size:13px">视图
      <select id="rd-view" onchange="rdChangeView()" style="padding:4px">
        <option value="grid">网格</option>
        <option value="artist">按作者</option>
        <option value="tags">按标签</option>
        <option value="parody">按原作</option>
        <option value="character">按角色</option>
        <option value="group_name">按社团</option>
        <option value="category">按分类</option>
        <option value="language">按语言</option>
      </select>
    </label>
    <label style="font-size:13px">排序
      <select id="rd-sort" onchange="rdChangeSort()" style="padding:4px">
        <option value="date">上传时间</option>
        <option value="name">名称</option>
        <option value="pages">页数</option>
      </select>
      <select id="rd-order" onchange="rdChangeSort()" style="padding:4px">
        <option value="desc">降序</option>
        <option value="asc">升序</option>
      </select>
    </label>
    <span id="rd-count" style="font-size:12px;color:#666"></span>
  </div>
</div>
<div class="card">
  <div class="row" style="justify-content:space-between;margin-bottom:4px">
    <b>筛选</b>
    <select id="rd-join" onchange="rdSaveOpts()" style="font-size:13px;padding:5px">
      <option value="and">满足全部</option>
      <option value="or">满足任一</option>
    </select>
  </div>
  <div class="row rd-filter-row" style="margin:0;gap:4px">
    <select id="rd-field" onchange="rdSaveOpts()" style="flex:0 0 52px;min-width:52px;font-size:13px;padding:6px 0;
      text-align:center;text-align-last:center;-webkit-appearance:none;appearance:none">
      <option value="tags">标签</option>
      <option value="artist">作者</option>
      <option value="group_name">社团</option>
      <option value="parody">原作</option>
      <option value="character">角色</option>
      <option value="category">分类</option>
      <option value="language">语言</option>
      <option value="title">标题</option>
    </select>
    <input type="text" id="rd-quick" placeholder="输入筛选, 回车收藏"
           oninput="rdQuickInput()" onkeydown="rdQuickKey(event)"
           style="flex:1;min-width:60px;font-size:13px;padding:6px">
    <button type="button" class="outline" id="rd-manage-btn"
            onclick="rdToggleFavManage()" style="flex:0 0 auto;font-size:12px;padding:6px 8px;white-space:nowrap">管理</button>
    <button type="button" class="outline" onclick="rdAddCond()"
            style="flex:0 0 auto;font-size:12px;padding:6px 8px;white-space:nowrap">+条件</button>
  </div>
  <div class="rd-fav" id="rd-favs"></div>
  <div id="rd-conds"></div>
</div>
<div class="rd-grid" id="rd-grid"></div>
<div id="rd-more" style="text-align:center;margin:10px 0"></div>
</div>

<!-- ===== 扫描页 ===== -->
<div id="page-scan" class="page">
<h2>📦 扫描漫画</h2>
<div class="card">
  <label>扫描目录 (逗号分隔多个)</label>
  <div class="row">
    <input type="text" id="scan-dir" value="/storage/emulated/0/Download" oninput="syncDirFromScan()">
    <button class="outline" onclick="dirHistToggle('scan')" style="flex:0 0 auto;padding:6px 8px">历史</button>
  </div>
  <div id="scan-dir-hist-box" class="rd-hist-pop" style="display:none"></div>
  <label style="display:block;margin-top:8px">数据库文件</label>
  <input type="text" id="scan-db" value="__DB_PATH__">
  <div class="row" style="margin-top:8px">
    <label><input type="checkbox" id="scan-recursive"> 扫描子目录</label>
    <label><input type="checkbox" id="rescan"> 强制重扫</label>
    <button onclick="doScan()">开始扫描</button>
    <button class="outline" id="btn-stop" onclick="doStop()" disabled>停止</button>
  </div>
  <div style="font-size:12px;color:#666;margin-top:6px">
    不勾「扫描子目录」时只看这一层, 子目录里的文件不会被扫到。
    <br>也可以填 WebDAV 远程路径, 例如 <code>123:/本图/本子zip</code>
    (本地和远程不能混在同一次扫描里, 请分开扫)。
  </div>
  <div class="progress hidden" id="scan-prog"><div class="progress-fill" style="width:0"></div></div>
  <!-- 进度固定一行, 原地刷新。远程每个文件要一两秒, 不能靠日志滚动
       表达进度(930 个文件会刷成瀑布), 所以单独一行显示"已处理 N/M"。 -->
  <div id="scan-progress-line"
       style="font-size:13px;color:#555;margin:4px 0;display:none"></div>
  <div id="scan-status">就绪</div>
</div>
<div class="card">
  <b>日志</b>
  <div class="log" id="scan-log">等待扫描...</div>
</div>
</div>

<!-- ===== 浏览页 ===== -->
<div id="page-browse" class="page hidden">
<h2>🔍 浏览数据</h2>
<div class="card">
  <div class="row">
    <select id="browse-scope" onchange="onScopeChange()">
      <option value="scan">本次扫描到的</option>
      <option value="all">全部库</option>
    </select>
    <span id="browse-scope-hint" style="font-size:12px;color:#666"></span>
  </div>
  <div class="row">
    <select id="browse-field" onchange="loadDistinct()">
      <option value="artist">作者</option>
      <option value="group_name">社团</option>
      <option value="parody">原作</option>
      <option value="character">角色</option>
      <option value="language">语言</option>
      <option value="category">分类</option>
      <option value="tags">标签包含</option>
      <option value="title">标题包含</option>
    </select>
    <select id="browse-value"><option value="">(先点刷新)</option></select>
    <button class="outline" onclick="loadDistinct()">刷新值</button>
  </div>
  <div class="row">
    <input type="text" id="browse-contains" placeholder="或输入包含文本">
    <button onclick="doQuery()">查询</button>
  </div>
  <div id="browse-count" style="font-size:12px;color:#666;margin-bottom:8px"></div>
</div>
<div class="card" style="overflow-x:auto">
  <table id="browse-table">
    <thead><tr><th>文件名</th><th>作者</th><th>原作</th><th>语言</th><th>页数</th><th>标签</th></tr></thead>
    <tbody></tbody>
  </table>
</div>
</div>

<!-- ===== 分类页 ===== -->
<div id="page-classify" class="page hidden">
<h2>📂 分类整理</h2>

<div class="card">
  <b>1. 处理哪些文件</b>
  <div style="margin-top:8px">
    <label style="font-size:12px;color:#666">源目录 (只处理这里的文件,库里没有的忽略)</label>
    <input type="text" id="cls-src" value="/storage/emulated/0/Download"
           oninput="syncDest();updateRemoteNote()">
    <label style="display:block;margin-top:6px">
      <input type="checkbox" id="cls-recursive"> 递归子目录
    </label>
  </div>
</div>

<div class="card">
  <div class="row" style="justify-content:space-between;margin-bottom:6px">
    <b>2. 筛选条件</b>
    <select id="cls-join" style="font-size:12px;padding:4px">
      <option value="and">满足全部 (并且)</option>
      <option value="or">满足任一 (或者)</option>
    </select>
  </div>
  <div id="cls-conds"></div>
  <button class="outline" onclick="addCond()" style="margin-top:6px">+ 添加条件</button>
</div>

<div class="card">
  <b>3. 目标位置</b>
  <label style="display:block;margin-top:8px;font-size:12px;color:#666">基础目录</label>
  <input type="text" id="cls-dest" value="/storage/emulated/0/Download"
         oninput="destTouched=true;updateRemoteNote()">
  <label style="display:block;margin-top:8px;font-size:12px;color:#666">按字段建子目录
    <span style="color:#999">(跟随筛选条件第 1 条, 可手动改)</span></label>
  <select id="cls-groupby" onchange="updateDestHint()">
    <option value="artist">按作者</option>
    <option value="parody">按原作</option>
    <option value="group_name">按社团</option>
    <option value="language">按语言</option>
    <option value="character">按角色</option>
    <option value="tags">按标签</option>
    <option value="category">按分类</option>
    <option value="format">按格式</option>
  </select>
  <label style="display:block;margin-top:8px">
    <input type="checkbox" id="cls-groupdir" onchange="updateDestHint()" checked>
    多包一层字段名文件夹
  </label>
  <div style="font-size:11px;color:#888;margin-top:4px" id="cls-dest-hint"></div>
</div>

<div class="card">
  <b>4. 执行</b>
  <div class="row" style="margin-top:8px">
    <label><input type="radio" name="mode" value="copy" id="cls-mode-copy"> 复制</label>
    <label><input type="radio" name="mode" value="move" checked> 移动</label>
    <label><input type="checkbox" id="cls-preview" checked> 先预览</label>
  </div>
  <div id="cls-remote-note" style="font-size:12px;color:#e65100;margin-top:6px"></div>
  <div class="row">
    <button onclick="doPreview()">预览</button>
    <button onclick="doClassify()" id="cls-run">直接执行</button>
    <button class="outline" id="cls-stop" onclick="doClsStop()" disabled>停止</button>
  </div>
  <div class="progress hidden" id="cls-prog"><div class="progress-fill" style="width:0"></div></div>
  <div class="log hidden" id="cls-log"></div>
</div>

<div class="card hidden" id="cls-preview-card">
  <div class="row" style="justify-content:space-between">
    <b>预览结果</b>
    <span id="cls-count" style="font-size:12px;color:#666"></span>
  </div>
  <div id="cls-missing" style="font-size:12px;color:#e65100;margin:6px 0"></div>
  <div style="max-height:340px;overflow:auto;margin-top:6px">
    <table id="cls-table">
      <thead><tr><th>文件名</th><th>→ 目标</th></tr></thead>
      <tbody></tbody>
    </table>
  </div>
  <div class="row" style="margin-top:10px">
    <button onclick="doClassify()">确认执行</button>
    <button class="outline" onclick="hidePreview()">取消</button>
  </div>
</div>
</div>

<!-- ===== 统计页 ===== -->
<div id="page-stats" class="page hidden">
<h2>📊 统计</h2>
<div class="card" id="stats-overview">
  <div class="row">
    <select id="stats-scope" onchange="onStatScopeChange()">
      <option value="scan">本次扫描到的</option>
      <option value="all">全部库</option>
    </select>
    <span id="stats-scope-hint" style="font-size:12px;color:#666"></span>
  </div>
  <div id="stats-summary" style="margin-top:8px"></div>
  <div class="row" style="margin-top:8px">
    <button onclick="loadStats()">刷新统计</button>
  </div>
</div>

<div class="card">
  <label style="font-size:12px;color:#666">选择字段看分布</label>
  <div id="stats-fields" style="display:flex;flex-wrap:wrap;gap:6px;margin-top:6px"></div>
</div>

<div class="card" id="stats-detail-card" style="display:none">
  <div class="row" style="justify-content:space-between">
    <b id="stats-detail-title">分布</b>
    <span id="stats-detail-meta" style="font-size:12px;color:#666"></span>
  </div>
  <div class="row" style="margin-top:6px">
    <input type="text" id="stats-q" placeholder="搜索值…" oninput="onStatSearch()">
    <select id="stats-sort" onchange="loadStatField(true)" style="flex:0 0 auto">
      <option value="count">按数量</option>
      <option value="name">按名称</option>
    </select>
  </div>
  <div id="stats-bars"></div>
  <div class="row" style="margin-top:10px">
    <button class="outline" id="stats-more" onclick="statLoadMore()">显示更多</button>
    <button class="outline" id="stats-all" onclick="statLoadAll()">全部加载</button>
  </div>
</div>

<!-- 点分布条后在这里展开作品列表, 不跳页 -->
<div class="card" id="stats-drill" style="display:none">
  <div class="row" style="justify-content:space-between">
    <b id="stats-drill-title">作品</b>
    <button class="outline" onclick="closeStatDrill()">返回分布</button>
  </div>
  <div id="stats-drill-body" style="margin-top:6px"></div>
</div>
</div>

<!-- ===== 上传页 ===== -->
<div id="page-upload" class="page hidden">
  <h2>⬆️ 上传</h2>
  <div class="card">
    <div style="font-size:12px;color:#666;margin-bottom:6px">本地漫画压缩包所在目录</div>
    <div class="row" style="position:relative">
      <input type="text" id="up-local" placeholder="本地路径">
      <button class="outline" onclick="upHistToggle('local')" style="flex:0 0 auto;padding:6px 8px">历史</button>
    </div>
    <div id="up-local-hist-box" class="rd-hist-pop" style="display:none"></div>
    <div class="row" style="font-size:13px;align-items:center;margin-top:4px">
      <label><input type="checkbox" id="up-recursive"> 递归扫描本地子目录</label>
    </div>

    <div style="font-size:12px;color:#666;margin:10px 0 4px">
      <b>上传目标</b>：本地文件最终传到这个远程目录
    </div>
    <div class="row" style="position:relative">
      <input type="text" id="up-target" placeholder="上传目标远程路径">
      <button class="outline" onclick="upHistToggle('target')" style="flex:0 0 auto;padding:6px 8px">历史</button>
    </div>
    <div id="up-target-hist-box" class="rd-hist-pop" style="display:none"></div>

    <div style="font-size:12px;color:#666;margin:10px 0 4px">
      <b>比对范围</b>：用这个远程目录下的文件列表判断是否重复（含子目录）
    </div>
    <div class="row" style="position:relative">
      <input type="text" id="up-compare" placeholder="远程比对范围路径">
      <button class="outline" onclick="upHistToggle('compare')" style="flex:0 0 auto;padding:6px 8px">历史</button>
    </div>
    <div id="up-compare-hist-box" class="rd-hist-pop" style="display:none"></div>

    <div class="row" style="margin-top:10px">
      <button onclick="upPrepare()">获取文件列表并比对</button>
      <button class="outline" onclick="upStart()">开始上传</button>
    </div>
    <div id="up-summary" style="font-size:12px;color:#666;margin-top:6px"></div>
    <div id="up-progress-box" style="display:none;margin-top:10px">
      <div class="progress"><div id="up-progress-fill" class="progress-fill" style="width:0%"></div></div>
      <div id="up-progress-text" style="font-size:12px;color:#666;margin-top:4px"></div>
    </div>
    <div id="up-log" style="font-size:12px;line-height:1.6;margin-top:8px;max-height:220px;overflow:auto"></div>
  </div>
  <div class="card">
    <div class="row" style="justify-content:space-between">
      <b>本地无元数据 <span id="up-nometa-count" style="color:#ff9800"></span></b>
      <span>
        <button class="outline" onclick="upNoMetaAll()" style="background:#ff9800;color:#fff;border-color:#ff9800">全部加入待上传</button>
      </span>
    </div>
    <div id="up-nometa-list" style="font-size:12px;line-height:1.6"></div>
    <div id="up-nometa-pager" style="font-size:12px;color:#666;text-align:center;margin-top:6px"></div>
  </div>
  <div class="card">
    <div class="row" style="justify-content:space-between">
      <b>待上传 <span id="up-upload-count" style="color:#1976d2"></span></b>
      <button class="outline" id="up-upload-more" onclick="upMoreAll('upload')" style="display:none">展开更多</button>
    </div>
    <div id="up-upload-list" style="font-size:12px;line-height:1.6"></div>
    <div id="up-upload-pager" style="font-size:12px;color:#666;text-align:center;margin-top:6px"></div>
  </div>
  <div class="card">
    <div class="row" style="justify-content:space-between">
      <b>待删除本地重复文件 <span id="up-delete-count" style="color:#c62828"></span></b>
      <span>
        <button class="outline" onclick="upDeleteAll()" style="background:#c62828;color:#fff;border-color:#c62828">删除全部</button>
        <button class="outline" id="up-delete-more" onclick="upMoreAll('delete')" style="display:none">展开更多</button>
      </span>
    </div>
    <div id="up-delete-list" style="font-size:12px;line-height:1.6"></div>
    <div id="up-delete-pager" style="font-size:12px;color:#666;text-align:center;margin-top:6px"></div>
  </div>
  <div class="card hidden" id="up-delete-confirm-card" style="border:2px solid #c62828">
    <div class="row" style="justify-content:space-between">
      <b style="color:#c62828">确认删除本地重复文件</b>
      <button class="outline" onclick="closeUpDeleteModal()">关闭</button>
    </div>
    <div id="up-delete-confirm-list" style="font-size:12px;line-height:1.6"></div>
    <div class="row" style="justify-content:flex-end;margin-top:10px">
      <button class="outline" onclick="closeUpDeleteModal()">取消</button>
      <button onclick="confirmUpDelete()" style="background:#c62828">确认删除</button>
    </div>
  </div>
</div>

</div>

<!-- 详情弹窗(默认隐藏, 点文件名才弹出) -->
<div id="detail-modal" onclick="if(event.target===this)closeDetail()">
  <div class="card">
    <div class="row" style="justify-content:space-between">
      <b>详情</b>
      <span>
        <button class="outline" onclick="mdApply()" style="background:#1976d2;color:#fff">应用筛选</button>
        <button class="outline" onclick="closeDetail()">关闭</button>
      </span>
    </div>
    <div id="detail-content" style="margin-top:8px"></div>
    <div class="row" style="justify-content:flex-end;margin-top:10px">
      <button class="outline" onclick="closeDetail()">关闭</button>
    </div>
  </div>
</div>

<div id="dup-modal" onclick="if(event.target===this)closeDupModal()">
  <div class="card" style="max-height:80vh;overflow:auto">
    <div class="row" style="justify-content:space-between">
      <b>重复文件确认</b>
      <button class="outline" onclick="closeDupModal()">关闭</button>
    </div>
    <div id="dup-list" style="font-size:12px;line-height:1.6"></div>
    <div class="row" style="justify-content:flex-end;margin-top:10px">
      <button class="outline" onclick="closeDupModal()">取消</button>
      <button onclick="confirmDeleteDups()" style="background:#c62828">删除后者</button>
    </div>
  </div>
</div>

<!-- 底部导航 -->
<nav class="nav">
  <a onclick="showPage('read')" id="nav-read"><span class="icon">📖</span>阅读</a>
  <a onclick="showPage('scan')" id="nav-scan"><span class="icon">📦</span>扫描</a>
  <a onclick="showPage('browse')" id="nav-browse"><span class="icon">🔍</span>浏览</a>
  <a onclick="showPage('classify')" id="nav-classify"><span class="icon">📂</span>分类</a>
  <a onclick="showPage('stats')" id="nav-stats"><span class="icon">📊</span>统计</a>
  <a onclick="showPage('upload')" id="nav-upload"><span class="icon">⬆️</span>上传</a>
</nav>

<!-- 阅读器弹层 -->
<div id="reader-modal">
  <div class="rd-top">
    <button onclick="rdClose()">← 返回</button>
    <span class="rd-title" id="rd-view-title"></span>
    <span id="rd-page-no" style="font-size:12px"></span>
    <button class="mode" id="rd-mode-btn" onclick="rdCycleMode()">→→</button>
  </div>
  <div class="rd-stage" id="rd-stage"
       onclick="rdStageClick(event)"
       ontouchstart="rdTouchStart(event)"
       ontouchend="rdTouchEnd(event)"></div>
</div>

<script>
function clientLog(stage,data){
  try{
    console.log('[CLIENT] '+stage,data||{});
    fetch('/api/client/log',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({stage:stage,data:data||{},t:Date.now()}),keepalive:true})
      .catch(()=>{});
  }catch(e){console.log('[CLIENT] 日志失败',stage,e)}
}

let dupCandidates=[];
function closeDupModal(){document.getElementById('dup-modal').classList.remove('show')}
function showDupModal(list){
  dupCandidates=list||[];
  if(!dupCandidates.length)return;
  const box=document.getElementById('dup-list');
  box.innerHTML=dupCandidates.map((g,i)=>
    '<div style="margin-bottom:10px"><b>'+esc(g.name)+'</b> '+
    (g.size?'<span style="color:#666">'+fmtSize(g.size)+'</span>':'')+
    (g.reason==='suffix'?' <span style="color:#ff9800">[序号重复, 必须删除]</span>':'')+'<br>'+
    '<span style="color:#388e3c">保留: '+esc(g.keep)+'</span><br>'+
    g.delete.map(p=>'<span style="color:#c62828">删除: '+esc(p)+'</span>').join('<br>')+
    '</div>').join('');
  document.getElementById('dup-modal').classList.add('show');
}
function confirmDeleteDups(){
  const paths=[];dupCandidates.forEach(g=>paths.push(...g.delete));
  if(!paths.length){closeDupModal();return}
  const must=dupCandidates.filter(g=>g.reason==='suffix').length;
  if(!confirm('确认删除 '+paths.length+' 个文件？'+(must?'\n其中 '+must+' 组是序号重复文件, 必须删除。':'')))return;
  fetch('/api/duplicates/delete',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({paths:paths})}).then(r=>r.json()).then(d=>{
      closeDupModal();
      alert('删除完成: 成功 '+(d.ok||0)+' 失败 '+(d.fail||0));
    });
}
let upPlanId=null, upUploadId=null, upPollTimer=null, upDeletePaths=[];
let upUploadItems=[], upDeleteItems=[], upNoMetaItems=[], upNoMetaLimit=10;
let upGroupLimits={upload:{},delete:{}};
const UP_HIST_KEYS={local:'manga_upload_local_hist',target:'manga_upload_target_hist',compare:'manga_upload_compare_hist'};
function upV(id){const e=document.getElementById(id);return e?e.value.trim():''}
function upHist(which){try{return JSON.parse(localStorage.getItem(UP_HIST_KEYS[which])||'[]')}catch(e){return []}}
function upHistPush(which,v){
  v=(v||'').trim();if(!v)return;
  const h=upHist(which).filter(x=>x!==v);h.unshift(v);
  try{localStorage.setItem(UP_HIST_KEYS[which],JSON.stringify(h.slice(0,20)))}catch(e){}
}
function upHistRender(which){
  const box=document.getElementById('up-'+which+'-hist-box');if(!box)return;
  const h=upHist(which);
  box.innerHTML=h.length?h.map(v=>
    '<div class="rd-hi" onclick="upHistPick(\''+which+'\',\''+encodeURIComponent(v)+'\')">'+esc(v)+'</div>').join(''):
    '<div class="rd-hi" style="color:#999">暂无历史, 比对后会记住当前路径</div>';
}
function upHistToggle(which){
  const box=document.getElementById('up-'+which+'-hist-box');if(!box)return;
  if(box.style.display==='none'){upHistRender(which);box.style.display='block'}
  else box.style.display='none';
}
function upHistPick(which,enc){
  const v=decodeURIComponent(enc);
  document.getElementById('up-'+which).value=v;
  document.querySelectorAll('#page-upload .rd-hist-pop').forEach(e=>e.style.display='none');
}
function upRenderList(kind){
  const items=kind==='upload'?upUploadItems:upDeleteItems;
  const limits=upGroupLimits[kind]||(upGroupLimits[kind]={});
  const box=document.getElementById('up-'+kind+'-list');
  const pager=document.getElementById('up-'+kind+'-pager');
  const more=document.getElementById('up-'+kind+'-more');
  document.getElementById('up-'+kind+'-count').textContent='('+items.length+')';
  const groups={};
  items.forEach(x=>{(groups[x.reason]||(groups[x.reason]=[])).push(x)});
  let totalShown=0, hasMore=false;
  box.innerHTML=Object.keys(groups).map(reason=>{
    const arr=groups[reason];
    const lim=limits[reason]||10;
    const take=Math.min(arr.length,lim);
    totalShown+=take;
    if(arr.length>take)hasMore=true;
    return '<div style="font-weight:bold;color:#555;margin:6px 0 2px">'+esc(reason)+
      ' ('+arr.length+')</div>'+
      arr.slice(0,take).map(x=>
        '<div style="padding-left:8px">'+esc(x.name)+' '+
        '<span style="color:#666">'+fmtSize(x.size)+'</span></div>').join('')+
      (arr.length>take?'<div style="padding-left:8px;color:#1976d2;cursor:pointer" '+
        'onclick="upMoreOne(\''+kind+'\',\''+encodeURIComponent(reason)+'\')">'+
        '本组还有 '+(arr.length-take)+' 个, 点这里再展开一段</div>':'');
  }).join('')||'无';
  if(hasMore){
    more.style.display='inline-block';
    more.textContent='展开更多(每组各加10个)';
  }else{
    more.style.display='none';
  }
  pager.textContent=items.length?('显示 '+totalShown+' / '+items.length+' 个'):'';
}
function upRenderNoMeta(){
  const items=upNoMetaItems;
  const box=document.getElementById('up-nometa-list');
  const pager=document.getElementById('up-nometa-pager');
  document.getElementById('up-nometa-count').textContent='('+items.length+')';
  box.innerHTML=items.slice(0,upNoMetaLimit).map(x=>
    '<div>'+esc(x.name)+' <span style="color:#666">'+fmtSize(x.size)+'</span> '+
    '<button class="outline" style="padding:2px 8px;font-size:12px" onclick="upNoMetaAdd(\''+encodeURIComponent(x.path)+'\')">加入待上传</button> '+
    '<button class="outline" style="padding:2px 8px;font-size:12px" onclick="upNoMetaIgnore(\''+encodeURIComponent(x.path)+'\')">忽略×</button></div>').join('')||'无';
  pager.textContent=items.length>upNoMetaLimit?'显示 '+Math.min(upNoMetaLimit,items.length)+' / '+items.length:'';
}
function upNoMetaAdd(enc){
  const path=decodeURIComponent(enc);
  const idx=upNoMetaItems.findIndex(x=>x.path===path);
  if(idx<0)return;
  const it=upNoMetaItems.splice(idx,1)[0];
  it.action='upload';it.reason='手动加入(本地无元数据)';
  upUploadItems.push(it);
  upRenderNoMeta();upRenderList('upload');
}
function upNoMetaIgnore(enc){
  const path=decodeURIComponent(enc);
  upNoMetaItems=upNoMetaItems.filter(x=>x.path!==path);
  upRenderNoMeta();
}
function upNoMetaAll(){
  if(!upNoMetaItems.length)return;
  upNoMetaItems.forEach(x=>{x.action='upload';x.reason='手动加入(本地无元数据)';upUploadItems.push(x)});
  upNoMetaItems=[];
  upRenderNoMeta();upRenderList('upload');
}
function upMoreOne(kind, reasonEnc){
  const reason=decodeURIComponent(reasonEnc);
  if(!upGroupLimits[kind])upGroupLimits[kind]={};
  upGroupLimits[kind][reason]=(upGroupLimits[kind][reason]||10)+10;
  upRenderList(kind);
}
function upMoreAll(kind){
  const items=kind==='upload'?upUploadItems:upDeleteItems;
  const groups={};
  items.forEach(x=>{(groups[x.reason]||(groups[x.reason]=[])).push(x)});
  Object.keys(groups).forEach(reason=>{
    if(!upGroupLimits[kind])upGroupLimits[kind]={};
    upGroupLimits[kind][reason]=(upGroupLimits[kind][reason]||10)+10;
  });
  upRenderList(kind);
}
function upDeleteAll(){
  if(!upDeleteItems.length){alert('没有待删除文件');return}
  if(!confirm('确认删除全部 '+upDeleteItems.length+' 个本地重复文件？'))return;
  fetch('/api/upload/delete',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({paths:upDeleteItems.map(x=>x.path)})}).then(r=>r.json()).then(d=>{
      const removed=new Set((d.results||[]).filter(x=>x[1]==='ok').map(x=>x[0]));
      upDeleteItems=upDeleteItems.filter(x=>!removed.has(x.path));
      upRenderList('delete');
      alert('删除完成: 成功 '+(d.ok||0)+' 失败 '+(d.fail||0));
    });
}
function upV(id){const e=document.getElementById(id);return e?e.value.trim():''}
function upPrepare(){
  const body={local_dir:upV('up-local'),recursive:document.getElementById('up-recursive').checked,
    target_root:upV('up-target'),compare_root:upV('up-compare')};
  document.getElementById('up-summary').textContent='正在比对...';
  fetch('/api/upload/prepare',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify(body)}).then(r=>r.json()).then(d=>{
      if(d.error){alert(d.error);document.getElementById('up-summary').textContent=d.error;return}
      if(d.remote_count===0 && !confirm('警告: 比对范围内一个远程文件都没读到(remote_count=0)。\n请确认远程路径填对了。仍要继续吗？'))return;
      upPlanId=d.plan_id;upUploadId=null;
      upUploadItems=d.upload||[];upDeleteItems=d.delete||[];upNoMetaItems=d.no_meta||[];
      upGroupLimits={upload:{},delete:{}};upNoMetaLimit=10;
      upHistPush('local',body.local_dir);upHistPush('target',body.target_root);
      upHistPush('compare',body.compare_root);
      document.getElementById('up-summary').textContent=
        '本地 '+d.local_count+' 个, 已重命名 '+(d.renamed_count||0)+' 个, '+
        '无元数据 '+d.no_meta_count+' 个, 待上传 '+d.upload_count+' 个, 待删除 '+d.delete_count+' 个';
      upRenderNoMeta();upRenderList('upload');upRenderList('delete');
      document.getElementById('up-log').innerHTML='';
    });
}
function upStart(){
  if(!upPlanId){alert('请先获取文件列表并比对');return}
  fetch('/api/upload/start',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({plan_id:upPlanId})}).then(r=>r.json()).then(d=>{
      if(d.error){alert(d.error);return}
      upUploadId=d.upload_id;
      document.getElementById('up-progress-box').style.display='block';
      document.getElementById('up-progress-fill').style.width='0%';
      document.getElementById('up-progress-text').textContent='准备中...';
      document.getElementById('up-summary').textContent='上传中...';
      upPoll();
    });
}
function upPoll(){
  if(!upUploadId)return;
  fetch('/api/upload/status?id='+encodeURIComponent(upUploadId)).then(r=>r.json()).then(d=>{
    if(d.error){document.getElementById('up-log').textContent=d.error;return}
    const pg=d.progress||{};
    if(pg.total){
      const pct=Math.round((pg.done||0)/(pg.total||1)*100);
      document.getElementById('up-progress-fill').style.width=pct+'%';
      document.getElementById('up-progress-text').textContent=
        (pg.phase||'')+'  '+(pg.done||0)+' / '+pg.total+
        (pg.current?('  '+pg.current):'');
    }
    const logs=d.log||[];
    document.getElementById('up-log').innerHTML=logs.slice(-120).map(esc).join('<br>');
    document.getElementById('up-log').scrollTop=document.getElementById('up-log').scrollHeight;
    if(d.running){upPollTimer=setTimeout(upPoll,800);return}
    const r=d.result||{};
    document.getElementById('up-summary').textContent=
      '上传完成: 成功 '+(r.ok||0)+' 失败 '+(r.fail||0);
    document.getElementById('up-progress-text').textContent='完成';
    if(r.delete&&r.delete.length)showUpDeleteModal(r.delete);
  });
}
function showUpDeleteModal(list){
  upDeletePaths=(list||[]).map(x=>x.path);
  document.getElementById('up-delete-confirm-list').innerHTML=(list||[]).map(x=>
    '<div>'+esc(x.name)+' <span style="color:#c62828">'+esc(x.reason)+'</span><br>'+
    '<span style="color:#666">'+esc(x.path)+'</span></div>').join('');
  const card=document.getElementById('up-delete-confirm-card');
  card.classList.remove('hidden');
  card.scrollIntoView({behavior:'smooth',block:'start'});
}
function closeUpDeleteModal(){document.getElementById('up-delete-confirm-card').classList.add('hidden')}
function confirmUpDelete(){
  if(!upDeletePaths.length){closeUpDeleteModal();return}
  if(!confirm('确认删除 '+upDeletePaths.length+' 个本地重复文件？'))return;
  fetch('/api/upload/delete',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({paths:upDeletePaths})}).then(r=>r.json()).then(d=>{
      closeUpDeleteModal();
      alert('删除完成: 成功 '+(d.ok||0)+' 失败 '+(d.fail||0));
    });
}
let scanId=null, clsId=null, pollTimer=null, clsPollTimer=null;

// 字段名 -> 中文(统计页和分类页共用)
const FIELD_CN_STAT={artist:'作者',group_name:'社团',parody:'原作',
  character:'角色',tags:'标签',language:'语言',category:'分类',
  format:'格式',title:'标题',title_original:'原标题',page_count:'页数',
  translated:'翻译',year:'年',source_gid:'来源ID',other_fields:'其它字段'};

function showPage(p){
  document.querySelectorAll('.page').forEach(e=>e.classList.add('hidden'));
  document.getElementById('page-'+p).classList.remove('hidden');
  document.querySelectorAll('.nav a').forEach(a=>a.classList.remove('active'));
  document.getElementById('nav-'+p).classList.add('active');
  // 统计页每次进来都重新拉: 默认范围是"本次扫描到的", 刚扫完就切过来
  // 必须看到这次的新数字, 缓存住会一直显示上一次的
  if(p==='scan'){
    restoreScan();
    const v=localStorage.getItem('manga_scan_dir');
    const el=document.getElementById('scan-dir');
    if(v!==null&&el)el.value=v;
  }
  if(p==='stats')loadStats();
  // 浏览页同理: 进来就按当前范围(默认"本次扫描到的")显示
  if(p==='browse'){loadScopeHint();loadDistinct();doQuery();}
  if(p==='upload'){
    // 远程路径默认取各自历史里最近一条
    [['local','up-local'],['target','up-target'],['compare','up-compare']].forEach(pair=>{
      const el=document.getElementById(pair[1]);
      if(el && !el.value){
        const h=upHist(pair[0]);
        if(h.length)el.value=h[0];
      }
    });
  }
  if(p==='read'){
    rdRestoreOpts();rdRenderFavs();dirHistRender('rd');
    const key=rdDir()+'|'+(document.getElementById('rd-subdirs')?.checked?'1':'0');
    if(!rdLoaded||rdLoadedKey!==key)rdLoad();
  }
}

function toggleSettings(){const m=document.getElementById('settings-modal');
  if(!m)return;
  const show=!m.classList.contains('show');
  m.classList.toggle('show',show);
  if(show){
    try{const v=localStorage.getItem('manga_cache_max_mb');
      if(v)document.getElementById('rd-cache-max').value=v;}catch(e){}
    rdLoadCacheStats();
  }
}
// ---- 阅读缓存设置 ----
function rdCacheMax(){const v=parseInt(document.getElementById('rd-cache-max')?.value)||500;return v}
function rdSaveCacheMax(){
  const mb=rdCacheMax();
  try{localStorage.setItem('manga_cache_max_mb',String(mb))}catch(e){}
  fetch('/api/cache/max',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({max_mb:mb})})
    .then(r=>r.json()).then(()=>rdLoadCacheStats());
}
function rdClearCache(){fetch('/api/cache/clear',{method:'POST'}).then(r=>r.json()).then(d=>{
  if(d.ok){rdLoadCacheStats();alert('缓存已清空')}})}
function rdLoadCacheStats(){fetch('/api/cache/stats?max_mb='+rdCacheMax()).then(r=>r.json()).then(d=>{
  const el=document.getElementById('rd-cache-stats');if(!el)return;
  if(d.error){el.textContent=d.error;return}
  el.textContent='已缓存 '+d.count+' 本 / '+(d.bytes/1048576).toFixed(1)+' MB';})}

// ---- 阅读 ----
const RD_FIELDS={tags:'标签',artist:'作者',group_name:'社团',parody:'原作',
  character:'角色',category:'分类',language:'语言',title:'标题'};
let rdFavs=JSON.parse(localStorage.getItem('manga_rd_favs')||'[]');
let rdSel=[], rdItems=[], rdView=null, rdPageIdx=0, rdQuickTimer=null, rdHistoryPushed=false, rdDelTimer=null, rdManageFavs=false;
let rdExpanded=new Set(), rdGroupPages={}, rdListPage=0, rdDirTotal=0, rdPageReq=0, rdLoaded=false, rdLoadedKey="";
const RD_PAGE_SIZE=30;
function rdSortMode(){const el=document.getElementById('rd-sort');
  const v=el?el.value:(localStorage.getItem('manga_rd_sort')||'date');
  return ['name','date','pages'].includes(v)?v:'date'}
function rdOrder(){const el=document.getElementById('rd-order');
  const v=el?el.value:(localStorage.getItem('manga_rd_order')||'desc');
  return v==='asc'?'asc':'desc'}
function rdChangeSort(){localStorage.setItem('manga_rd_sort',rdSortMode());
  localStorage.setItem('manga_rd_order',rdOrder());
  rdListPage=0;rdExpanded.clear();rdGroupPages={};rdSortItems();rdRenderView()}
function rdItemCmp(a,b){const mode=rdSortMode(), sign=rdOrder()==='asc'?1:-1;
  const dateKey=it=>{const y=Number(it.year)||0,m=Number(it.month)||0,d=Number(it.day)||0;
    return y*10000+m*100+d};
  if(mode==='date')return sign*(dateKey(a)-dateKey(b))||rdNatural(a.name,b.name);
  if(mode==='pages')return sign*((Number(a.page_count)||0)-(Number(b.page_count)||0))||
    rdNatural(a.name,b.name);
  return sign*rdNatural(a.name,b.name)}
function rdSortItems(){rdItems.sort(rdItemCmp)}
function rdViewMode(){const v=localStorage.getItem('manga_rd_view')||'grid';
  return document.getElementById('rd-view')&&v!=='grid'?v:'grid'}
function rdChangeView(){const v=document.getElementById('rd-view').value;
  localStorage.setItem('manga_rd_view',v);rdExpanded.clear();rdGroupPages={};rdRenderView()}
function rdNatural(a,b){const re=/(\d+)/g,A=String(a).split(re),B=String(b).split(re);
  for(let i=0;i<Math.min(A.length,B.length);i++){
    const x=A[i],y=B[i];
    if(/^\d+$/.test(x)&&/^\d+$/.test(y)){const d=parseInt(x)-parseInt(y);if(d)return d}
    else if(x.toLowerCase()!==y.toLowerCase())return x.toLowerCase()<y.toLowerCase()?-1:1;
  }return A.length-B.length}
function rdSplit(raw,field){const out=[],seen={};
  for(const part of String(raw||'').split(/[,，]/)){
    const v=part.trim();if(!v||/^unknown$/i.test(v)||seen[v])continue;
    if(field==='parody'&&/^original$/i.test(v))continue;
    seen[v]=1;out.push(v);
  }return out}
function rdGroups(){const field=rdViewMode();const map={};
  for(const it of rdItems){
    const vals=field==='title'||field==='grid'?[]:rdSplit(it[field],field);
    for(const v of vals.length?vals:['未分类'])(map[v]=map[v]||[]).push(it);
  }
  // 按每组作品数降序; 数量相同按名称自然序, 未分类永远最后
  return Object.keys(map).sort((a,b)=>
    a==='未分类'?1:b==='未分类'?-1:map[b].length-map[a].length||rdNatural(a,b))
    .map(label=>({label,items:map[label].sort(rdItemCmp)}))}
function rdRenderGroups(){const grid=document.getElementById('rd-grid');
  // 分组模式: 一组一行, 不受"每行 2/3/4 本"影响
  grid.classList.remove('rd-grid');grid.style.gridTemplateColumns='';
  grid.classList.add('rd-group-list');
  const groups=rdGroups();
  const prevSrc=rdCaptureSrc(grid);
  const openSet={};
  const start=rdListPage*RD_PAGE_SIZE;
  const pageGroups=groups.slice(start,start+RD_PAGE_SIZE);
  grid.innerHTML=pageGroups.map((g,i)=>{
    const gi=start+i;
    const exp=rdExpanded.has(g.label);
    const gp=rdGroupPages[g.label]||0;
    const gStart=gp*RD_PAGE_SIZE;
    const gItems=exp?g.items.slice(gStart,gStart+RD_PAGE_SIZE):g.items.slice(0,10);
    const cards=gItems.map(it=>{
      const idx=rdItems.indexOf(it);
      const open=it.open_name||it.name;
      const src=rdSrcAttr(prevSrc,open);
      if(src)openSet[open]=prevSrc[open];
      return '<div class="rd-gcard">'+
        '<img alt="" onclick="rdOpen('+idx+')" data-name="'+esc(open)+'" data-size="'+it.size+'"'+src+'>'+
        '<div class="rd-gname rd-meta-btn" onclick="rdShowDetail('+idx+')">'+esc(it.title_original||it.title||it.name)+'</div></div>';
    }).join('');
    let pager='';
    if(exp&&g.items.length>RD_PAGE_SIZE){
      const pages=Math.ceil(g.items.length/RD_PAGE_SIZE);
      pager='<div class="rd-gpager" style="padding:6px 10px;text-align:center">'+
        '<button class="outline" onclick="rdTurnGroup('+gi+',-1)"'+(gp<=0?' disabled':'')+'>上页</button> '+
        '<span style="font-size:12px;color:#666;margin:0 8px">'+(gp+1)+' / '+pages+'</span> '+
        '<button class="outline" onclick="rdTurnGroup('+gi+',1)"'+(gp>=pages-1?' disabled':'')+'>下页</button></div>';
    }
    const rest=exp?'<div class="rd-gexpand">'+gItems.map(it=>{
      const idx=rdItems.indexOf(it);
      const open=it.open_name||it.name;
      return '<div class="rd-card">'+
        '<img class="rd-thumb" style="cursor:pointer" onclick="rdOpen('+idx+')" alt="" data-name="'+esc(open)+'" data-size="'+it.size+'"'+rdSrcAttr(openSet,open)+'>'+
        '<div class="rd-meta rd-meta-btn" onclick="rdShowDetail('+idx+')">'+
        '<div class="rd-name">'+esc(it.title_original||it.title||it.name)+'</div></div></div>';
    }).join('')+pager+'</div>':'';
    return '<div class="rd-grow" id="rd-group-row-'+gi+'"><div class="rd-glabel" title="'+esc(g.label)+'">'+esc(g.label)+'('+g.items.length+')</div>'+
      '<div class="rd-gstrip">'+cards+'</div>'+
      '<button class="rd-gmore" onclick="rdToggleGroup('+gi+')">'+(exp?'收起':'展开')+'</button></div>'+rest;
  }).join('')||'<div class="rd-loading">没有符合条件的漫画</div>';
  rdFillThumbs(grid);
  rdPager(Math.ceil(groups.length/RD_PAGE_SIZE),
    '共 '+groups.length+' 组 / '+rdItems.length+' 本');}
function rdTurnGroup(i,d){
  const g=rdGroups()[i];if(!g)return;
  const label=g.label;
  const pages=Math.ceil(g.items.length/RD_PAGE_SIZE);
  const cur=rdGroupPages[label]||0;
  rdGroupPages[label]=Math.max(0,Math.min(cur+d,pages-1));
  rdRenderGroups();
}
function rdToggleGroup(i){
  const g=rdGroups()[i];
  if(!g)return;
  const label=g.label;
  if(rdExpanded.has(label))rdExpanded.delete(label);
  else rdExpanded.add(label);
  rdRenderGroups();
}
function rdCaptureSrc(root){const m={};
  root.querySelectorAll('img[data-name]').forEach(el=>{
    const src=el.getAttribute('src');
    if(src)m[el.dataset.name]=src;
  });return m}
function rdSrcAttr(prev,key){return prev&&prev[key]?' src="'+esc(prev[key])+'"':''}
const THUMB_PORT=__THUMB_PORT__;
function thumbUrl(name,size){
  return 'http://'+location.hostname+':'+THUMB_PORT+
    '/api/reader/thumb?'+new URLSearchParams({
      name:name,size:size,src_dir:rdDir()});
}
function rdFillThumbs(root){let k=0;let timer=null;
  const imgs=[...root.querySelectorAll('img[data-name]')];
  function next(){
    if(document.getElementById('reader-modal').classList.contains('show')){
      timer=setTimeout(next,600);return;
    }
    if(k>=imgs.length)return;
    const img=imgs[k++];
    if(img.getAttribute('src'))return next();
    img.src=thumbUrl(img.dataset.name,img.dataset.size);
    img.onerror=()=>{img.style.background='#f8f8f8';next()};
    img.onload=next;
    timer=setTimeout(next,600);
  }next()}
function rdRenderView(){const v=rdViewMode();
  const grid=document.getElementById('rd-grid');
  if(v==='grid'){grid.classList.add('rd-grid');grid.classList.remove('rd-group-list');
    rdRenderGrid();return}
  rdRenderGroups()}

function rdDir(){return document.getElementById('rd-dir').value.trim()}
function scanDir(){return document.getElementById('scan-dir').value.trim()}
const DIR_KEY='manga_rd_dir';
function syncDirFromRead(){try{localStorage.setItem(DIR_KEY,rdDir());localStorage.setItem('manga_scan_dir',rdDir())}catch(e){}
  const el=document.getElementById('scan-dir');if(el&&el.value!==rdDir())el.value=rdDir()}
function syncDirFromScan(){try{localStorage.setItem('manga_scan_dir',scanDir());localStorage.setItem(DIR_KEY,scanDir())}catch(e){}
  const el=document.getElementById('rd-dir');if(el&&el.value!==scanDir())el.value=scanDir()}
function rdSaveOpts(){
  try{
    localStorage.setItem('manga_rd_join',document.getElementById('rd-join').value);
    localStorage.setItem('manga_rd_field',document.getElementById('rd-field').value);
    localStorage.setItem('manga_rd_subdirs',document.getElementById('rd-subdirs').checked?'1':'0');
  }catch(e){}
  syncDirFromRead();
}
function rdRestoreOpts(){
  const set=(id,key,def)=>{const el=document.getElementById(id);if(!el)return;
    const v=localStorage.getItem(key);if(v!==null)el.value=v;else if(def!==undefined)el.value=def};
  set('rd-cols','manga_rd_cols','2');
  set('rd-join','manga_rd_join','and');
  set('rd-field','manga_rd_field','tags');
  {
    // 两页路径修改时互相同步; 但刷新时各自恢复自己上次的值,
    // 不用共享值互相覆盖, 否则在扫描页扫完别的目录后刷新阅读页会换目录。
    const v=localStorage.getItem('manga_rd_dir');
    if(v!==null){const r=document.getElementById('rd-dir');if(r)r.value=v;}
  }
  set('rd-view','manga_rd_view','grid');
  set('rd-sort','manga_rd_sort','date');
  set('rd-order','manga_rd_order','desc');
  const sub=document.getElementById('rd-subdirs');if(sub)sub.checked=localStorage.getItem('manga_rd_subdirs')==='1';
  rdApplyCols();
}
const RD_DIR_HIST_KEY='manga_rd_dirs', RD_DIR_HIST_MAX=10;
function rdDirHist(){
  try{const a=JSON.parse(localStorage.getItem(RD_DIR_HIST_KEY)||'[]');
    return Array.isArray(a)?a.filter(x=>typeof x==='string'&&x.trim()):[];
  }catch(e){return []}
}
function rdDirHistPush(v){v=(v||'').trim();if(!v)return;
  const out=[v].concat(rdDirHist().filter(x=>x!==v)).slice(0,RD_DIR_HIST_MAX);
  try{localStorage.setItem(RD_DIR_HIST_KEY,JSON.stringify(out))}catch(e){}
}
function dirHistRender(which){const box=document.getElementById(which+'-dir-hist-box');
  if(!box)return;
  const h=rdDirHist();
  box.innerHTML=h.length?h.map(v=>
    '<div class="rd-hi" onclick="dirHistPick(\''+encodeURIComponent(v)+'\')">'+esc(v)+'</div>').join(''):
    '<div class="rd-hi" style="color:#999">暂无历史, 刷新或扫描后会记住当前路径</div>'}
function dirHistToggle(which){const box=document.getElementById(which+'-dir-hist-box');
  if(!box)return;
  if(box.style.display==='none'){dirHistRender(which);box.style.display='block'}
  else box.style.display='none'}
function dirHistPick(enc){const v=decodeURIComponent(enc);
  document.getElementById('rd-dir').value=v;
  document.getElementById('scan-dir').value=v;
  try{localStorage.setItem(DIR_KEY,v);localStorage.setItem('manga_scan_dir',v)}catch(e){}
  document.querySelectorAll('.rd-hist-pop').forEach(e=>e.style.display='none');
  if(!document.getElementById('page-read').classList.contains('hidden'))rdLoad();
}
function rdRefresh(){rdSaveOpts();rdDirHistPush(rdDir());rdLoad()}
function rdSaveFavs(){localStorage.setItem('manga_rd_favs',JSON.stringify(rdFavs))}
function rdChipKey(f,v){return f+'\u0001'+v}
function rdRenderFavs(){
  const box=document.getElementById('rd-favs');
  box.innerHTML=rdFavs.length?rdFavs.map((f,i)=>{
    const on=rdSel.some(s=>rdChipKey(s.field,s.value)===rdChipKey(f.field,f.value));
    const del=rdManageFavs?
      '<button type="button" class="rd-del" aria-label="删除标签" '+
      'onclick="rdDelFav(event,'+i+')">×</button>':'';
    return '<span class="rd-chip fav'+(on?' on':'')+'">'+
      '<span class="rd-label" onclick="rdToggleFav('+i+')">'+
      esc(RD_FIELDS[f.field]||f.field)+':'+esc(f.value)+'</span>'+del+'</span>';
  }).join(''):'<span style="font-size:12px;color:#aaa">输入文字筛选; 回车可收藏成标签</span>';
}
function rdToggleFavManage(){
  rdManageFavs=!rdManageFavs;
  const b=document.getElementById('rd-manage-btn');
  if(b)b.textContent=rdManageFavs?'完成':'管理';
  // 退出管理时复位所有待确认的删除按钮
  document.querySelectorAll('#rd-favs .rd-del').forEach(b=>{
    b.dataset.confirm='';b.textContent='×';b.classList.remove('confirm');
  });
  clearTimeout(rdDelTimer);rdDelTimer=null;
  rdRenderFavs();
}
function rdDelFav(e,i){
  e.stopPropagation();
  const btn=e.currentTarget;
  if(btn.dataset.confirm!=='1'){
    // 第一次点: 进入确认状态, 不真删; 同时给其它标签复位
    document.querySelectorAll('#rd-favs .rd-del').forEach(b=>{
      b.dataset.confirm='';b.textContent='×';b.classList.remove('confirm');
    });
    btn.dataset.confirm='1';btn.textContent='确认删除';btn.classList.add('confirm');
    clearTimeout(rdDelTimer);
    rdDelTimer=setTimeout(()=>{
      if(btn.isConnected){btn.dataset.confirm='';btn.textContent='×';btn.classList.remove('confirm')}
    },3500);
    return;
  }
  // 第二次点: 真删
  clearTimeout(rdDelTimer);rdDelTimer=null;
  const f=rdFavs[i], k=rdChipKey(f.field,f.value);
  rdSel=rdSel.filter(s=>rdChipKey(s.field,s.value)!==k);
  rdFavs.splice(i,1);rdSaveFavs();rdRenderFavs();rdLoad();
}
function rdToggleFav(i){
  const f=rdFavs[i], k=rdChipKey(f.field,f.value);
  const at=rdSel.findIndex(s=>rdChipKey(s.field,s.value)===k);
  if(at>=0)rdSel.splice(at,1);
  else rdSel.push({field:f.field,value:f.value,op:'contains'});
  rdRenderFavs();rdLoad();
}
function rdQuickInput(){
  if(rdQuickTimer)clearTimeout(rdQuickTimer);
  rdQuickTimer=setTimeout(rdLoad,200);   // 本地条件, 只发筛选请求, 不查词表
}
function rdQuickKey(e){
  if(e.key!=='Enter')return;
  e.preventDefault();
  const f=document.getElementById('rd-field').value;
  const v=document.getElementById('rd-quick').value.trim();
  if(!v)return;
  const k=rdChipKey(f,v);
  if(!rdFavs.some(x=>rdChipKey(x.field,x.value)===k)){
    rdFavs.push({field:f,value:v});rdSaveFavs();
  }
  if(!rdSel.some(s=>rdChipKey(s.field,s.value)===k))
    rdSel.push({field:f,value:v,op:'contains'});
  document.getElementById('rd-quick').value='';
  rdRenderFavs();rdLoad();
}
function rdRemoveCond(id){const el=document.getElementById(id);if(el)el.remove();rdLoad()}
let rdCondTimer=null;
function rdCondInput(){if(rdCondTimer)clearTimeout(rdCondTimer);rdCondTimer=setTimeout(rdLoad,250)}
function rdAddCondRow(field,op,value){
  const box=document.getElementById('rd-conds');
  const id='rdc'+Date.now()+Math.floor(Math.random()*100000);
  const div=document.createElement('div');div.className='cond';div.id=id;
  div.innerHTML='<span class="cond-idx"></span>'+
    '<select class="c-field" onchange="rdLoad()">'+
    Object.keys(RD_FIELDS).map(f=>'<option value="'+f+'">'+RD_FIELDS[f]+'</option>').join('')+
    '</select><select class="c-op" onchange="rdLoad()">'+
    '<option value="eq">等于</option><option value="contains">包含</option>'+
    '<option value="ne">不等于</option><option value="not_contains">不包含</option>'+
    '<option value="empty">为空</option><option value="not_empty">不为空</option>'+
    '</select><input type="text" class="c-val" placeholder="值" oninput="rdCondInput()">'+
    '<button class="del" onclick="rdRemoveCond(\''+id+'\')">×</button>';
  box.appendChild(div);
  if(field)div.querySelector('.c-field').value=field;
  if(op)div.querySelector('.c-op').value=op;
  if(value)div.querySelector('.c-val').value=value;
  return div;
}
function rdAddCond(){rdAddCondRow()}
function rdConditions(){
  const out=rdSel.map(s=>({field:s.field,op:'contains',value:s.value}));
  const q=document.getElementById('rd-quick');
  if(q&&q.value.trim())
    out.push({field:document.getElementById('rd-field').value,
              op:'contains',value:q.value.trim()});
  document.querySelectorAll('#rd-conds .cond').forEach(el=>{
    const c={field:el.querySelector('.c-field').value,
      op:el.querySelector('.c-op').value,
      value:el.querySelector('.c-val').value.trim()};
    if(c.value||c.op==='empty'||c.op==='not_empty')out.push(c);
  });
  return out;
}
function rdLoad(force){
  rdListPage=0; rdExpanded.clear(); rdGroupPages={};
  const v=rdViewMode(); const sel=document.getElementById('rd-view');
  if(sel)sel.value=v;
  const sortSel=document.getElementById('rd-sort');
  if(sortSel)sortSel.value=rdSortMode();
  const orderSel=document.getElementById('rd-order');
  if(orderSel)orderSel.value=rdOrder();
  const body={src_dir:rdDir(),conditions:rdConditions(),
    join:document.getElementById('rd-join').value,
    scope:'all',limit:100000,force:!!force,
    subdirs:!!document.getElementById('rd-subdirs')?.checked};
  fetch('/api/reader/list',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify(body)}).then(r=>r.json()).then(d=>{
      if(d.error||!Array.isArray(d.items)){
        document.getElementById('rd-grid').innerHTML=
          '<div class="rd-loading">'+esc(d.error||'接口返回格式错误')+'</div>';
        return;
      }
      rdItems=d.items; rdDirTotal=d.in_dir||0;
      rdLoaded=true;
      rdLoadedKey=rdDir()+'|'+(document.getElementById('rd-subdirs')?.checked?'1':'0');
      rdSortItems();
      document.getElementById('rd-count').textContent=
        d.items.length+' / 共 '+d.total+' 本 (目录里 '+d.in_dir+' 本)';
      rdRenderView();
    });
}
function rdApplyCols(){
  const n=parseInt(document.getElementById('rd-cols').value)||2;
  try{localStorage.setItem('manga_rd_cols',String(n))}catch(e){}
  document.getElementById('rd-grid').style.gridTemplateColumns='repeat('+n+',1fr)';
}
function rdPager(totalPages,countDesc){
  document.getElementById('rd-more').innerHTML=totalPages<=1?countDesc:
    '<button class="outline" onclick="rdTurn(-1)"'+(rdListPage<=0?' disabled':'')+'>上页</button> '+
    '<span style="font-size:12px;color:#666;margin:0 8px">'+(rdListPage+1)+' / '+totalPages+' ('+countDesc+')</span> '+
    '<button class="outline" onclick="rdTurn(1)"'+(rdListPage>=totalPages-1?' disabled':'')+'>下页</button>'}
function rdTurn(d){rdListPage+=d;rdExpanded.clear();rdGroupPages={};rdRenderView();
  window.scrollTo({top:document.getElementById('rd-grid').offsetTop,behavior:'instant'})}
function rdRenderGrid(){
  rdApplyCols();
  const grid=document.getElementById('rd-grid');
  const prevSrc=rdCaptureSrc(grid);
  const start=rdListPage*RD_PAGE_SIZE;
  const pageItems=rdItems.slice(start,start+RD_PAGE_SIZE);
  grid.innerHTML=pageItems.map((it,i)=>{
    const idx=start+i;
    const sub=it.page_count?('共 '+it.page_count+' 页'):'页数未知';
    const date=it.year?((it.year||'')+'-'+String(it.month||0).padStart(2,'0')+'-'+String(it.day||0).padStart(2,'0')):'';
    return '<div class="rd-card">'+
      '<img class="rd-thumb" style="cursor:pointer" onclick="rdOpen('+idx+')" loading="lazy" alt="" data-name="'+esc(it.open_name||it.name)+'" data-size="'+it.size+'"'+rdSrcAttr(prevSrc,it.open_name||it.name)+'>' +
      '<div class="rd-meta rd-meta-btn" onclick="rdShowDetail('+idx+')" title="查看元数据并筛选">'+
      '<div class="rd-name">'+esc(it.title_original||it.title||it.name)+'</div>'+
      '<div class="rd-meta-row"><span>'+esc(sub)+'</span><span class="rd-date">'+esc(date)+'</span></div></div></div>';
  }).join('')||'<div class="rd-loading">没有符合条件的漫画</div>';
  rdFillThumbs(grid);
  rdPager(Math.ceil(rdItems.length/RD_PAGE_SIZE),
    '共 '+rdItems.length+' 本 (目录里 '+rdDirTotal+' 本)');
}
function rdOpen(i){
  rdView=rdItems[i];
  clientLog('RD_OPEN_BEGIN',{i:i,name:rdView.open_name||rdView.name,
    size:rdView.size||0,src_dir:rdDir()});
  // 点开就立刻进全屏, 不等 pages 接口: 首屏先给个轻量加载提示
  history.pushState({mangaReader:1},'');rdHistoryPushed=true;
  document.documentElement.classList.add('rd-open');
  document.body.classList.add('rd-open');
  document.getElementById('reader-modal').classList.add('show');
  document.getElementById('rd-view-title').textContent=rdView.title_original||rdView.title||rdView.name;
  document.getElementById('rd-page-no').textContent='加载中…';
  document.getElementById('rd-mode-btn').textContent=
    (RD_MODES.find(m=>m[0]===rdMode())||RD_MODES[0])[1];
  const opened=rdView;
  // 首帧直接用已缓存的缩略图(后台预热过, 本地秒出), 不等整包下载
  const thumb=thumbUrl(opened.open_name||opened.name,opened.size||0);
  document.getElementById('rd-stage').innerHTML=
    '<img src="'+esc(thumb)+'" style="max-width:100%;max-height:100%;object-fit:contain" data-manga="'+esc(opened.open_name||opened.name)+'" data-thumb="1">';
  clientLog('RD_OPEN_THUMB',{src:thumb});
  const pagesUrl='/api/reader/pages?'+new URLSearchParams({
    name:opened.open_name||opened.name,src_dir:rdDir(),size:opened.size||0});
  clientLog('RD_PAGES_FETCH',{url:pagesUrl});
  fetch(pagesUrl)
    .then(r=>{clientLog('RD_PAGES_RESPONSE',{status:r.status,ok:r.ok});return r.json()})
    .then(d=>{
      clientLog('RD_PAGES_JSON',{error:d.error||null,pages:Array.isArray(d.pages)?d.pages.length:null,
        first:(d.pages&&d.pages[0])||null});
      if(d.error){alert(d.error);rdClose();return;}
      if(rdView!==opened){clientLog('RD_PAGES_STALE',{});return;}
      rdView.pages=d.pages;rdPageIdx=0;
      rdRenderPage();
    })
    .catch(e=>clientLog('RD_PAGES_FAIL',{error:String(e)}));
}
// 翻页模式: 'lr' 左到右(左=上一页), 'rl' 右到左(右=上一页), 'scroll' 上下连续
const RD_MODES=[['lr','→→'],['rl','←←'],['scroll','↕↕']];
function rdMode(){return localStorage.getItem('manga_rd_mode')||'lr'}
function rdCycleMode(){
  const i=RD_MODES.findIndex(m=>m[0]===rdMode());
  const m=RD_MODES[(i+1)%RD_MODES.length];
  localStorage.setItem('manga_rd_mode',m[0]);
  document.getElementById('rd-mode-btn').textContent=m[1];
  if(rdView){
    if(m[0]==='scroll')rdRenderScroll(true);
    else rdRenderPage();
  }
}
function rdPageUrl(entry){
  return '/api/reader/page?'+new URLSearchParams({
    name:rdView.open_name||rdView.name,src_dir:rdDir(),entry:entry,size:rdView.size||0});
}
function rdRenderPage(){
  if(!rdView)return;
  const stage=document.getElementById('rd-stage');
  const mode=rdMode();
  stage.classList.toggle('scroll-mode',false);
  // seq 防止快速连点时旧页的回调覆盖新页
  const seq=(rdPageReq||0)+1;rdPageReq=seq;
  const url=rdPageUrl(rdView.pages[rdPageIdx]);
  const context={seq:seq,idx:rdPageIdx,entry:rdView.pages[rdPageIdx],
    name:rdView.open_name||rdView.name,url:url};
  clientLog('RD_RENDER_BEGIN',context);
  const apply=()=>{
    if(rdPageReq!==seq){clientLog('RD_RENDER_STALE',context);return;}
    let img=stage.querySelector('img');
    if(!img||img.dataset.manga!==(rdView.open_name||rdView.name)){
      stage.innerHTML='<img data-manga="'+esc(rdView.open_name||rdView.name)+'">';
      img=stage.querySelector('img');
    }
    if(img.getAttribute('src')!==url)img.src=url;
    clientLog('RD_RENDER_APPLY',Object.assign({},context,{src:img.getAttribute('src')}));
  };
  // 让可见 img 自己加载：onload 前保留缩略图，绝不在等待期间清空成黑屏。
  let img=stage.querySelector('img');
  if(!img||img.dataset.manga!==(rdView.open_name||rdView.name)){
    stage.innerHTML='<img data-manga="'+esc(rdView.open_name||rdView.name)+'">';
    img=stage.querySelector('img');
  }
  img.dataset.seq=String(seq);
  img.onload=()=>{
    clientLog('RD_IMG_LOADED',Object.assign({},context,
      {w:img.naturalWidth,h:img.naturalHeight,src:img.getAttribute('src')}));
    apply();
  };
  img.onerror=()=>clientLog('RD_IMG_ERROR',Object.assign({},context,
    {src:img.getAttribute('src')}));
  if(img.getAttribute('src')!==url){
    clientLog('RD_IMG_SET_SRC',context);
    img.src=url;
  }else{
    clientLog('RD_IMG_SRC_SAME',context);
    apply();
  }
  // 顺手预加载后一页(只填浏览器缓存, 不改当前显示)
  const next=rdPageIdx+1;
  if(next<rdView.pages.length){const p=new Image();p.src=rdPageUrl(rdView.pages[next])}
  stage.scrollTop=0;
  document.getElementById('rd-page-no').textContent=
    (rdPageIdx+1)+' / '+rdView.pages.length;
  document.getElementById('rd-mode-btn').textContent=
    (RD_MODES.find(m=>m[0]===mode)||RD_MODES[0])[1];
}
// 连续滚动模式: 一次渲染前后各几页, 滚到接近末尾再追加
function rdRenderScroll(reset){
  if(!rdView)return;
  const stage=document.getElementById('rd-stage');
  stage.classList.add('scroll-mode');
  if(reset){
    stage.innerHTML=rdView.pages.map((p,i)=>
      '<img data-p="'+i+'"'+(i<8?' src="'+rdPageUrl(p)+'"':'')+'>').join('');
    rdPageIdx=0;
    stage.scrollTop=0;
  }
  const imgs=stage.querySelectorAll('img');
  imgs.forEach(img=>{
    if(img.getAttribute('src'))return;
    const p=parseInt(img.dataset.p);
    if(Math.abs(p-rdPageIdx)<=8)img.src=rdPageUrl(rdView.pages[p]);
  });
  const at=Math.max(0,Math.min(rdView.pages.length-1,
    Math.round(stage.scrollTop/(stage.scrollHeight/rdView.pages.length))));
  rdPageIdx=at;
  document.getElementById('rd-page-no').textContent=
    (rdPageIdx+1)+' / '+rdView.pages.length;
  document.getElementById('rd-mode-btn').textContent='↕↕';
}
function rdOnScroll(){
  if(!rdView||rdMode()!=='scroll')return;
  const stage=document.getElementById('rd-stage');
  const at=Math.max(0,Math.min(rdView.pages.length-1,
    Math.round(stage.scrollTop/(stage.scrollHeight/rdView.pages.length))));
  rdPageIdx=at;
  document.getElementById('rd-page-no').textContent=(at+1)+' / '+rdView.pages.length;
  stage.querySelectorAll('img').forEach(img=>{
    if(img.getAttribute('src'))return;
    if(Math.abs(parseInt(img.dataset.p)-at)<=8)img.src=rdPageUrl(rdView.pages[parseInt(img.dataset.p)]);
  });
}
function rdPage(d){
  if(!rdView)return;
  if(rdMode()==='scroll'){
    const stage=document.getElementById('rd-stage');
    const per=stage.scrollHeight/rdView.pages.length;
    stage.scrollTop=Math.max(0,Math.min(stage.scrollHeight,
      stage.scrollTop+d*per));rdOnScroll();return;
  }
  const n=rdPageIdx+d;
  if(n<0||n>=rdView.pages.length)return;
  rdPageIdx=n;rdRenderPage();
}
// 点击左/右半屏翻页; scroll 模式不拦截点击
function rdStageClick(e){
  if(rdSwiped){rdSwiped=false;return;}   // 横滑后浏览器还会补一次 click, 别翻两页
  if(rdMode()==='scroll'||!rdView)return;
  const rect=document.getElementById('rd-stage').getBoundingClientRect();
  const left=e.clientX-rect.left<rect.width/2;
  const back=rdMode()==='lr'?left:!left;
  rdPage(back?-1:1);
}
let rdTouchX=null,rdTouchY=null,rdSwiped=false;
function rdTouchStart(e){
  rdTouchX=e.changedTouches[0].clientX;
  rdTouchY=e.changedTouches[0].clientY;
}
function rdTouchEnd(e){
  if(rdTouchX==null||rdMode()==='scroll')return;
  const dx=e.changedTouches[0].clientX-rdTouchX;
  const dy=e.changedTouches[0].clientY-rdTouchY;
  rdTouchX=rdTouchY=null;
  if(Math.abs(dx)<40||Math.abs(dx)<Math.abs(dy))return;  // 只认横向滑动
  rdSwiped=true;
  // 右滑=上一页 (lr) / 下一页 (rl); 左滑相反
  const forward=rdMode()==='lr'?(dx<0):(dx>0);
  rdPage(forward?1:-1);
}
function rdDoClose(){
  document.getElementById('reader-modal').classList.remove('show');
  document.getElementById('rd-stage').innerHTML='';
  document.documentElement.classList.remove('rd-open');
  document.body.classList.remove('rd-open');
  rdView=null;rdHistoryPushed=false;
}
function rdClose(){
  if(rdHistoryPushed){history.back();return;}
  rdDoClose();
}
window.addEventListener('popstate',()=>{
  if(document.getElementById('reader-modal').classList.contains('show'))rdDoClose();
});
document.addEventListener('keydown',e=>{
  if(!document.getElementById('reader-modal').classList.contains('show'))return;
  if(e.key==='ArrowLeft')rdPage(-1);
  if(e.key==='ArrowRight')rdPage(1);
  if(e.key==='Escape')rdClose();
});

// ---- 扫描 ----
function doScan(){rdDirHistPush(scanDir());
  const dirs=document.getElementById('scan-dir').value;
  const db=document.getElementById('scan-db').value;
  const rescan=document.getElementById('rescan').checked;
  const recursive=document.getElementById('scan-recursive').checked;
  fetch('/api/scan',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({dirs,db,rescan,recursive})})
    .then(r=>r.json()).then(d=>{
      scanId=d.scan_id;
      try{localStorage.setItem('manga_scan_id',scanId)}catch(e){}
      document.getElementById('scan-log').textContent='';
      logShown=0;
      document.getElementById('scan-status').textContent='扫描中...';
      document.getElementById('scan-prog').classList.remove('hidden');
      const pl=document.getElementById('scan-progress-line');
      pl.style.display='block';
      pl.textContent='准备中…';
      document.getElementById('btn-stop').disabled=false;
      pollScan();
    });
}
function doStop(){
  fetch('/api/scan/stop?id='+scanId).then(r=>r.json());
}

// 日志只追加新行。原来用 textContent.includes(l) 去重: 既 O(n²),
// 又会把"两行内容相同"误判成重复而吞掉。改成记住已渲染的行数。
let logShown=0;
function appendLog(lines){
  if(!lines||!lines.length)return;
  const box=document.getElementById('scan-log');
  if(logShown>lines.length)logShown=0;      // 换了新任务, 重新来
  if(logShown<lines.length){
    box.textContent+=lines.slice(logShown).join('\n')+'\n';
    logShown=lines.length;
  }
  box.scrollTop=box.scrollHeight;
}

// 进度行: 固定一行, 原地改数字, 不新增行。
function renderProgress(p,running){
  const el=document.getElementById('scan-progress-line');
  if(!el)return;
  if(!p){el.style.display='none';return;}
  el.style.display='block';
  const total=p.total||0, done=p.done||0;
  const pct=total? Math.min(100,Math.round(done*100/total)) : 0;
  let s='⏳ '+(p.phase||'处理中');
  // 拉清单/检查增量这类阶段还不知道总数, 不显示 N/M
  if(total>0)s+='  '+done+' / '+total+'  ('+pct+'%)';
  if(p.current)s+='  当前: '+p.current;
  el.textContent=s;
  const fill=document.querySelector('#scan-prog .progress-fill');
  if(fill&&total>0)fill.style.width=pct+'%';
}

function restoreScan(){
  let sid=null;
  try{sid=localStorage.getItem('manga_scan_id')}catch(e){}
  fetch('/api/scan/current').then(r=>r.json()).then(d=>{
    const id=d.scan_id||sid;
    if(!id)return;
    fetch('/api/scan/status?id='+encodeURIComponent(id)).then(r=>r.json()).then(s=>{
      if(s.error)return;
      scanId=id;
      document.getElementById('scan-log').textContent='';
      logShown=0;
      appendLog(s.log);
      renderProgress(s.progress,s.running);
      document.getElementById('scan-status').textContent=s.running?'扫描中...':'扫描已结束';
      document.getElementById('btn-stop').disabled=!s.running;
      if(s.running)pollScan();
    });
  });
}
function pollScan(){
  if(pollTimer)clearInterval(pollTimer);
  pollTimer=setInterval(()=>{
    fetch('/api/scan/status?id='+scanId).then(r=>r.json()).then(d=>{
      appendLog(d.log);
      renderProgress(d.progress,d.running);
      if(!d.running){
        clearInterval(pollTimer);pollTimer=null;
        try{localStorage.removeItem('manga_scan_id')}catch(e){}
        document.getElementById('btn-stop').disabled=true;
        document.getElementById('scan-prog').classList.add('hidden');
        const pl=document.getElementById('scan-progress-line');
        pl.style.display='none';
        if(d.stats){
          const s=d.stats;
          const st=document.getElementById('scan-status');
          if(s.error){
            // 扫描线程抛异常时 stats 只有 error, 没有 done/fail/skip。
            // 以前直接拼 s.done 会显示 "成功undefined 失败undefined"。
            st.textContent='扫描失败: '+s.error;
            st.style.color='#c62828';
          }else{
            st.textContent='完成 ✓ 成功'+s.done+' 失败'+s.fail+
              ' 跳过'+s.skip+' 耗时'+(s.elapsed||0).toFixed(0)+'s';
            st.style.color='';
          }
          if(s.duplicate_candidates&&s.duplicate_candidates.length){
            showDupModal(s.duplicate_candidates);
          }
        }
      }
    });
  },500);
}

// ---- 浏览 ----
function browseScope(){return document.getElementById('browse-scope').value}
function onScopeChange(){loadScopeHint();loadDistinct();doQuery();}
function loadScopeHint(){
  fetch('/api/scope').then(r=>r.json()).then(d=>{
    const el=document.getElementById('browse-scope-hint');
    if(browseScope()==='all'){
      el.textContent=d.scan_count!=null
        ? '(全库; 上次扫描到 '+d.scan_count+' 个)' : '(全库)';
    }else if(d.scan_count==null){
      el.textContent='(还没扫描过, 先到「扫描」页跑一次)';
    }else{
      el.textContent='(上次扫描 '+d.at+' 扫到 '+d.scan_count+' 个)';
    }
  });
}
function loadDistinct(){
  const f=document.getElementById('browse-field').value;
  fetch('/api/distinct?'+new URLSearchParams({field:f,scope:browseScope()}))
    .then(r=>r.json()).then(d=>{
    if(d.error){document.getElementById('browse-value').innerHTML=
      '<option value="">'+esc(d.error)+'</option>';return;}
    const sel=document.getElementById('browse-value');
    sel.innerHTML='<option value="">(全部)</option>'+
      d.values.map(v=>'<option value="'+esc(v.value)+'">'+esc(v.value)+' ('+v.count+')</option>').join('');
    // 下拉框只列前 500 个: 说清楚, 免得用户以为库里没有某个值
    if(d.truncated){
      sel.innerHTML+='<option value="" disabled>—— 共 '+d.total+
        ' 个值, 只列出前 '+d.values.length+' 个(用下面的"包含"搜索其余)——</option>';
    }
  });
}
function doQuery(){
  const f=document.getElementById('browse-field').value;
  const v=document.getElementById('browse-value').value;
  const c=document.getElementById('browse-contains').value;
  fetch('/api/query?'+new URLSearchParams({field:f,value:v,contains:c,
                                           scope:browseScope()}))
    .then(r=>r.json()).then(d=>{
      const tb=document.querySelector('#browse-table tbody');
      // 截断必须说出来: 只显示 count 的话分不清"就这么多"和"被截断了"
      document.getElementById('browse-count').textContent=
        d.truncated ? (d.count+' / 共 '+d.total+' 条(只显示前 '+d.limit+' 条)')
                    : (d.count+' 条');
      if(d.error){tb.innerHTML='<tr><td colspan=6>'+esc(d.error)+'</td></tr>';return;}
      tb.innerHTML=d.rows.map((r,i)=>{
        const m=cols2obj(d.columns,r);
        const vc=(d.value_counts&&d.value_counts[i])||{};
        // 多值字段: 显示前两个 + [N] 徽标, 点徽标展开全部
        const cell=(f,maxLen)=>{
          const raw=(m[f]==null?'':String(m[f]));
          const n=vc[f]||0;
          if(n>1){
            const parts=raw.split(',').map(x=>x.trim()).filter(Boolean);
            const brief=parts.slice(0,2).join(', ')+(parts.length>2?' …':'');
            return esc(brief)+' <span class="badge mv" data-vals="'+esc(raw)+
              '" data-n="'+n+'" onclick="event.stopPropagation();showVals(this)">'+n+'</span>';
          }
          return esc(raw.length>maxLen?raw.slice(0,maxLen)+'…':raw);
        };
        return '<tr onclick="showDetail('+JSON.stringify(m.name)+')">'+
          '<td>'+esc(m.name||'')+'</td>'+
          '<td>'+cell('artist',20)+'</td>'+
          '<td>'+cell('parody',20)+'</td>'+
          '<td>'+esc(m.language||'')+'</td>'+
          '<td>'+(m.page_count||'')+'</td>'+
          '<td>'+cell('tags',40)+'</td></tr>';
      }).join('');
    });
}
// 展开多值: 在徽标下方列出全部值, 再点收起
function showVals(el){
  const td=el.parentNode;
  const existing=td.querySelector('.vlist');
  if(existing){existing.remove();el.textContent=el.dataset.n;return;}
  const vals=(el.dataset.vals||'').split(',').map(s=>s.trim()).filter(Boolean);
  el.textContent='收起';
  const div=document.createElement('div');
  div.className='vlist';
  div.innerHTML=vals.map(v=>'<span class="tag">'+esc(v)+'</span>').join('');
  td.appendChild(div);
}
function cols2obj(cols,row){
  const o={};cols.forEach((c,i)=>o[c]=row[i]);return o;
}

// ---- 阅读页: 点文字区看元数据, 多选值加为筛选条件 ----
let mdPick=[];
function rdShowDetail(idx){
  const it=rdItems[idx];if(!it)return;
  mdPick=[];
  showDetail(it.name, true);
}
function mdToggle(field,value){
  const k=field+'\u0001'+value;
  const at=mdPick.findIndex(x=>x.k===k);
  if(at>=0)mdPick.splice(at,1);
  else mdPick.push({k:k,field:field,value:value});
  document.querySelectorAll('.md-chip').forEach(e=>e.classList.remove('on'));
  mdPick.forEach(x=>{
    const t=document.querySelector('.md-chip[data-k="'+CSS.escape(x.k)+'"]');
    if(t)t.classList.add('on');
  });
}
function mdApply(){
  if(!mdPick.length){closeDetail();return}
  for(const x of mdPick)rdAddCondRow(x.field,'contains',x.value);
  closeDetail();
  rdLoad();
}

// ---- 详情 ----
function showDetail(name, pickable){
  fetch('/api/detail?name='+encodeURIComponent(name)).then(r=>r.json()).then(d=>{
    const el=document.getElementById('detail-content');
    if(d.error){el.innerHTML='<p>'+esc(d.error)+'</p>';return;}
    let html='';
    const order=['name','path','size','image_count','has_meta','error','title',
      'title_original','artist','group_name','parody','character','tags',
      'category','language','translated','page_count','source_url','source_gid',
      'year','month','day','format','scanned_at'];
    for(const k of order){
      let v=d[k];
      if(k=='size'&&v)v=v+' ('+fmtSize(v)+')';
      if(k=='has_meta')v=v?'是':'否';
      if(k=='translated')v=v?'是':'否';
      if(v===null||v===undefined||v==='')continue;
      if(pickable&&RD_FIELDS[k]){
        const vals=String(v).split(/[,，]/).map(x=>x.trim()).filter(Boolean);
        html+='<div class="md-group"><b>'+(cnMap[k]||RD_FIELDS[k])+'</b>'+
          vals.map(x=>'<span class="md-chip" data-k="'+esc(k+'\u0001'+x)+
            '" onclick="mdToggle(\''+k+'\',\''+esc(x)+'\')">'+esc(x)+'</span>').join('')+'</div>';
      }else{
        html+='<div class="detail-pair"><div class="detail-key">'+
          (cnMap[k]||k)+'</div><div class="detail-val">'+esc(String(v))+'</div></div>';
      }
    }
    if(d.other_fields){
      const of=typeof d.other_fields=='string'?JSON.parse(d.other_fields):d.other_fields;
      for(const k in of){
        html+='<div class="detail-pair"><div class="detail-key">'+k+'</div>'+
          '<div class="detail-val">'+esc(String(of[k]))+'</div></div>';
      }
    }
    el.innerHTML=html;
    document.getElementById('detail-modal').classList.add('show');
  });
}
function closeDetail(){document.getElementById('detail-modal').classList.remove('show')}

// ---- 分类: 条件行 ----
const CLS_FIELDS=[['artist','作者'],['group_name','社团'],['parody','原作'],
  ['character','角色'],['language','语言'],['category','分类'],
  ['tags','标签'],['title','标题'],['format','格式']];
const CLS_OPS=[['eq','等于'],['ne','不等于'],['contains','包含'],
  ['not_contains','不包含'],['empty','为空'],['not_empty','不为空'],
  ['count_eq','= (作品数)'],['count_gte','>= (作品数)'],['count_lte','<= (作品数)']];
let condSeq=0, previewNames=null, lastPlan=null, destTouched=false;

// 目标路径提示: 跟着字段和"包一层"开关变
function updateDestHint(){
  const gb=document.getElementById('cls-groupby').value;
  const gd=document.getElementById('cls-groupdir').checked;
  const cn=FIELD_CN_STAT[gb]||gb;
  const base=document.getElementById('cls-dest').value.trim()||'基础目录';
  const path=gd ? base+'/'+cn+'/<'+cn+'值>/文件.zip'
                : base+'/<'+cn+'值>/文件.zip';
  document.getElementById('cls-dest-hint').textContent=
    '结果: '+path+',取不到值的放「未分类」';
}
// 目标基础目录默认跟随源目录; 用户手动改过就不再自动跟随
function syncDest(){
  if(destTouched)return;
  const s=document.getElementById('cls-src').value.trim();
  if(s)document.getElementById('cls-dest').value=s;
  updateDestHint();
  updateRemoteNote();
}

// 远程路径识别: `123:/...` 是远程。判定规则与服务端 source.is_remote 一致
// (排除 C:/ 这种盘符)。
function isRemotePath(p){
  if(!p||p.indexOf(':')<0)return false;
  const i=p.indexOf(':');
  const head=p.slice(0,i), rest=p.slice(i+1);
  if(!head||head.indexOf('/')>=0||head.indexOf('\\')>=0)return false;
  if(rest[0]!=='/')return false;
  if(head.length===1&&/[A-Za-z]/.test(head))return false;
  return true;
}
function updateRemoteNote(){
  const src=document.getElementById('cls-src').value.trim();
  const dst=document.getElementById('cls-dest').value.trim();
  const el=document.getElementById('cls-remote-note');
  const copyRadio=document.getElementById('cls-mode-copy');
  const rs=isRemotePath(src), rd=isRemotePath(dst);
  if(rs&&rd){
    el.textContent='远程模式: 仅支持移动, 不支持复制(123 云盘 WebDAV 的 COPY 返回 500)。'+
      '多值字段只会移动到第一个目标目录, 其余目录会跳过并在日志里列出。';
    // 远程不能复制: 直接把复制单选禁用, 免得选到一半才发现
    copyRadio.disabled=true;
    if(copyRadio.checked)document.querySelector('input[name=mode][value=move]').checked=true;
  }else if(rs||rd){
    el.textContent='⚠ 源目录和目标目录必须同为本地或同为远程(跨存储搬运暂不支持)。';
    copyRadio.disabled=false;
  }else{
    el.textContent='';
    copyRadio.disabled=false;
  }
}

// 第三个「按字段建子目录」单向跟随筛选条件的第一条: 二变则三变,
// 三手动改不影响二。只认第一行 —— 追加第二条不动三; 删掉第一条也不动三
// (第二条顶上来不算"新加的第一条")。没有条件时保持默认。
function syncGroupByFromFirstCond(){
  const first=document.querySelector('#cls-conds .cond');
  if(!first)return;
  const f=first.querySelector('.c-field').value;
  const sel=document.getElementById('cls-groupby');
  // 标题这类字段没有建目录选项, 保持三原样
  if([...sel.options].some(o=>o.value===f)){
    sel.value=f;
    updateDestHint();
  }
}
// ---- 分类: 条件值历史 (输入框右侧 ▾ 唤起, 最多显示 5 条) ----
// 数字条件(作品数)和文本条件分开记, 免得 "4" 混进作者名历史里
const CLS_HIST_KEY='clsValHist', CLS_HIST_MAX=5;
function clsHist(){
  try{
    const a=JSON.parse(localStorage.getItem(CLS_HIST_KEY)||'[]');
    return Array.isArray(a)?a.filter(e=>e&&typeof e.v==='string'):[];
  }catch(e){return []}
}
function clsHistSave(a){
  // 每类各留 5 条, 存 10 条, 显示时按当前条件类型筛
  const out=[];
  for(const e of a){
    const k=e.n?1:0;
    if(out.filter(x=>(x.n?1:0)===k).length<CLS_HIST_MAX)out.push(e);
  }
  try{localStorage.setItem(CLS_HIST_KEY,JSON.stringify(out))}catch(e){}
}
function clsHistPush(v,isCount){
  v=(v||'').trim();if(!v)return;
  const n=isCount?1:0;
  clsHistSave([{v:v,n:n}].concat(
    clsHist().filter(e=>!(e.v===v&&(e.n?1:0)===n))));
}
// 点浮层以外的地方收起所有历史下拉
document.addEventListener('click',e=>{
  if(e.target.closest('.c-val-wrap'))return;
  document.querySelectorAll('.hist-pop').forEach(p=>p.remove());
});
function setupValHistory(wrap){
  const input=wrap.querySelector('input'), btn=wrap.querySelector('.hist-btn');
  const isCount=()=>{
    const op=wrap.closest('.cond').querySelector('.c-op');
    return op&&op.value.startsWith('count_');
  };
  // 回车/失焦时记一条; 失焦延迟一拍, 让浮层里的点击先拿到
  input.addEventListener('keydown',e=>{if(e.key==='Enter')clsHistPush(input.value,isCount())});
  input.addEventListener('blur',()=>setTimeout(()=>clsHistPush(input.value,isCount()),150));
  btn.addEventListener('click',()=>{
    const had=wrap.querySelector('.hist-pop');
    document.querySelectorAll('.hist-pop').forEach(p=>p.remove());
    if(had)return;   // 再点一次收起
    const cnt=isCount();
    const vals=clsHist().filter(e=>!!e.n===cnt).map(e=>e.v).slice(0,CLS_HIST_MAX);
    const pop=document.createElement('div');
    pop.className='hist-pop';
    pop.innerHTML=vals.length
      ? '<div class="hint">最近 '+vals.length+' 条</div>'+vals.map(v=>
          '<div class="hi"><span class="hi-v">'+esc(v)+'</span>'+
          '<span class="hi-x" title="删除这条">×</span></div>').join('')
      : '<div class="hint">还没有历史记录</div>';
    wrap.appendChild(pop);
    const items=[...pop.querySelectorAll('.hi')];
    items.forEach((el,i)=>{
      const v=vals[i];
      el.addEventListener('click',ev=>{
        if(ev.target.classList.contains('hi-x')){
          clsHistSave(clsHist().filter(e=>e.v!==v));
          el.remove();
          if(!pop.querySelector('.hi'))
            pop.innerHTML='<div class="hint">还没有历史记录</div>';
          return;
        }
        input.value=v;
        input.dispatchEvent(new Event('input'));  // 手选的值不当自动填的默认
        pop.remove();input.focus();
      });
    });
  });
}
function addCond(field,op,value){
  const id=++condSeq;
  const div=document.createElement('div');
  div.className='cond';div.id='cond-'+id;
  div.innerHTML='<span class="cond-idx">'+(document.getElementById('cls-conds').children.length+1)+'</span>'+
    '<select class="c-field" onchange="doQuery()">'+CLS_FIELDS.map(f=>
      '<option value="'+f[0]+'"'+(f[0]===(field||'artist')?' selected':'')+'>'+f[1]+'</option>').join('')+'</select>'+
    '<select class="c-op">'+CLS_OPS.map(o=>
      '<option value="'+o[0]+'"'+(o[0]===(op||'eq')?' selected':'')+'>'+o[1]+'</option>').join('')+'</select>'+
    '<span class="c-val-wrap"><input type="text" class="c-val" placeholder="值" '+
      'value="'+esc(value||'')+'" autocomplete="off">'+
      '<button type="button" class="hist-btn" title="历史记录">▾</button></span>'+
    '<button class="del" onclick="delCond('+id+')">×</button>';
  document.getElementById('cls-conds').appendChild(div);
  // 操作符切换时更新输入框提示; 数量条件和"为空/不为空"的输入框行为不同
  const opSel=div.querySelector('.c-op'), valIn=div.querySelector('.c-val');
  let autoVal=false;   // 当前值是不是数量条件自动填的 4
  valIn.addEventListener('input',()=>{autoVal=false;});
  function syncVal(){
    const o=opSel.value;
    const isCount=o.startsWith('count_');
    valIn.placeholder=isCount?'数字':'值';
    valIn.disabled=(o==='empty'||o==='not_empty');
    if(isCount){
      // 数量条件默认给 4, 免得空着被当成 0 条作品
      if(!valIn.value){valIn.value='4';autoVal=true;}
    }else if(autoVal){
      // 离开数量条件时, 自动填的 4 清掉, 别让它跟到"等于作者"上
      valIn.value='';autoVal=false;
    }
    if(valIn.disabled)valIn.value='';
  }
  opSel.addEventListener('change',syncVal);
  syncVal();
  setupValHistory(div.querySelector('.c-val-wrap'));
  // 改的若是第一行字段, 三跟着变; 改第二行及以后不联动
  div.querySelector('.c-field').addEventListener('change',()=>{
    if(document.getElementById('cls-conds').firstElementChild===div)
      syncGroupByFromFirstCond();
  });
  updateCondIdx();
  // 只有"新加的这一行就是第一行"才联动(库里原本没条件);
  // 追加第二行时三保持用户当前选择
  if(document.getElementById('cls-conds').firstElementChild===div)
    syncGroupByFromFirstCond();
}
function delCond(id){
  const el=document.getElementById('cond-'+id);
  if(el)el.remove();
  updateCondIdx();
}
function updateCondIdx(){
  [...document.getElementById('cls-conds').children].forEach((el,i)=>{
    el.querySelector('.cond-idx').textContent=i+1;
  });
}
function collectConds(){
  const out=[];
  document.querySelectorAll('#cls-conds .cond').forEach(el=>{
    const op=el.querySelector('.c-op').value;
    out.push({
      field:el.querySelector('.c-field').value,
      op:op,
      value:el.querySelector('.c-val').value
    });
  });
  return out;
}
// 不预置条件行: 空条件(值没填)会被当成 artist = '' 从而把结果滤成 0 条,
// 看着就像"什么都没匹配到"。要筛自己点「+ 添加条件」。
updateDestHint();
updateRemoteNote();

// ---- 分类: 预览与执行 ----
function clsPayload(){
  return {
    conditions:collectConds(),
    join:document.getElementById('cls-join').value,
    base_dir:document.getElementById('cls-dest').value.trim(),
    group_by:document.getElementById('cls-groupby').value||'artist',
    group_dir:document.getElementById('cls-groupdir').checked,
    src_dir:document.getElementById('cls-src').value.trim()||null,
    recursive:document.getElementById('cls-recursive').checked,
    mode:document.querySelector('input[name=mode]:checked').value
  };
}
function doPreview(){
  const p=clsPayload();
  if(!p.src_dir){alert('请填源目录 —— 库不记路径, 要知道文件在哪才能算出目标');return;}
  if(!p.base_dir){alert('请填目标基础目录');return;}
  fetch('/api/preview',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify(p)})
    .then(r=>r.json()).then(d=>{
      if(d.error){alert('预览失败: '+d.error);return;}
      previewNames=d.plan.map(x=>x.name);
      lastPlan=d.plan;
      const tb=document.querySelector('#cls-table tbody');
      // 统计实际会写出多少份(多值字段会扇出)
      const fanout=d.plan.reduce((s,x)=>s+((x.targets&&x.targets.length)||1),0);
      document.getElementById('cls-count').textContent=
        d.count+' 个文件将被'+(p.mode=='move'?'移动':'复制')+
        (fanout>d.count?(' ('+fanout+' 份,有文件重复归档)'):'');
      const miss=document.getElementById('cls-missing');
      let warn=[];
      if(d.missing&&d.missing.length){
        warn.push('⚠ '+d.missing.length+' 个文件在磁盘上找不到,会被跳过');
      }
      // 扇出上限丢值: 必须明确提示, 否则用户以为该作品的值都归档了
      if(d.truncated&&d.truncated.length){
        warn.push('⚠ '+d.truncated.length+' 个作品的分类值超过单个作品 '+
          '12 个的上限, 多出的值不会建目录(如: '+
          esc((d.truncated[0].dropped||[]).slice(0,3).join(', '))+
          ((d.truncated[0].dropped||[]).length>3?' …':'')+')');
      }
      // 不同值清理后同名 -> 会并进同一目录, 必须提示
      if(d.merged&&d.merged.length){
        const m0=d.merged[0].merged[0]||[];
        warn.push('⚠ '+d.merged.length+' 个作品有"清理后同名"的分类值, '+
          '会被并进同一目录(如: '+esc(String(m0[0]))+' 与 '+
          esc(String(m0[1]))+')');
      }
      // 远程只能移到第一个目录: 扇出的作品必须提前说, 否则预览里
      // 看着每个目录都有, 实际只搬了第一个
      if(d.remote){
        const fan=lastPlan.filter(x=>(x.targets||[]).length>1).length;
        warn.push('⚠ 远程模式: 仅支持移动, 不支持复制');
        if(fan)warn.push('其中 '+fan+' 个作品有多个目标目录, '+
          '实际只会移到第一个, 其余会跳过');
      }
      miss.textContent=warn.join(' | ');
      const base=p.base_dir;
      // 文件名不截断: 完整显示, 靠 CSS 换行/横向滚动容纳(截断会让人
      // 以为是文件名本身有问题, 也对不上磁盘上的真实文件)
      const fullName=x=>esc(x.name||'');
      tb.innerHTML=d.plan.map(x=>{
        const tg=(x.targets&&x.targets.length)?x.targets:[x.dest_dir];
        const subOf=t=>(t&&t!==base)?t.slice(base.length).replace(/^\//,''):'(基础目录)';
        let destHtml='<span class="hl">'+esc(subOf(tg[0]))+'</span>/'+fullName(x);
        if(tg.length>1){
          destHtml+='<br>'+tg.slice(1).map(t=>
            '<span class="hl">'+esc(subOf(t))+'</span>/'+fullName(x)).join('<br>');
          destHtml+=' <span class="badge">共'+tg.length+'份</span>';
        }
        const bad=x.exists_src?'':' style="opacity:.45"';
        return '<tr'+bad+'><td>'+fullName(x)+'</td>'+
          '<td class="dest-path">'+destHtml+'</td></tr>';
      }).join('')||'<tr><td colspan=2>没有匹配的文件</td></tr>';
      document.getElementById('cls-preview-card').classList.remove('hidden');
      document.getElementById('cls-preview-card').scrollIntoView({behavior:'smooth'});
    });
}
function hidePreview(){
  document.getElementById('cls-preview-card').classList.add('hidden');
  previewNames=null;lastPlan=null;
}
function doClassify(){
  const p=clsPayload();
  if(!p.base_dir){alert('请填目标基础目录');return;}
  if(isRemotePath(p.src_dir)!==isRemotePath(p.base_dir)){
    alert('源目录与目标目录必须同为本地或同为远程(跨存储搬运暂不支持)');return;
  }
  if(isRemotePath(p.src_dir)&&p.mode!=='move'){
    alert('远程模式只支持移动, 不支持复制');return;
  }
  // 勾了"先预览"但还没预览过 -> 先预览
  if(document.getElementById('cls-preview').checked&&!previewNames){
    doPreview();return;
  }
  if(!lastPlan||!lastPlan.length){
    alert('没有要处理的文件,先预览一下');return;
  }
  // 执行前把当前各条件的值记进历史 (光填不跑的不算数, 免得历史被试错的乱值占满)
  p.conditions.forEach(c=>clsHistPush(c.value,c.op.startsWith('count_')));
  const names=lastPlan.map(x=>x.name);
  // 移动是不可逆的: 说明会生成几份,并明确原文件会被删
  let fanout=lastPlan.reduce((s,x)=>s+((x.targets&&x.targets.length)||1),0);
  if(p.mode=='move'){
    const extra=fanout>names.length
      ? '\n有多个作者/角色/标签的作品会在每个目录各放一份副本(共 '+fanout+' 份)。'
      : '';
    if(!confirm('确定要移动 '+names.length+' 个文件吗?'+extra+
        '\n原位置的文件会被删除,此操作不可撤销。'))return;
  }
  // names 一并送过去: 服务端用它当上界。上面确认框说的是"移动 N 个",
  // 若预览后条件被改过, 服务端重新推导的集合会更大 —— 那就可能搬走
  // 用户没看过、没确认过的文件。取交集保证只会更少。
  fetch('/api/classify',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({dest:p.base_dir,mode:p.mode,group_by:p.group_by,
      src_dir:p.src_dir,recursive:p.recursive,group_dir:p.group_dir,
      conditions:p.conditions,join:p.join,names:names})})
    .then(r=>r.json()).then(d=>{
      if(d.error){alert('执行失败: '+d.error);return;}
      clsId=d.cls_id;
      document.getElementById('cls-log').textContent='';
      document.getElementById('cls-log').classList.remove('hidden');
      document.getElementById('cls-stop').disabled=false;
      document.getElementById('cls-prog').classList.remove('hidden');
      document.getElementById('cls-run').disabled=true;
      pollCls();
    });
}
function doClsStop(){fetch('/api/classify/stop?id='+clsId)}
function pollCls(){
  if(clsPollTimer)clearInterval(clsPollTimer);
  clsPollTimer=setInterval(()=>{
    fetch('/api/classify/status?id='+clsId).then(r=>r.json()).then(d=>{
      if(d.log)d.log.forEach(l=>{
        if(!document.getElementById('cls-log').textContent.includes(l))
          document.getElementById('cls-log').textContent+=l+'\n';
      });
      document.getElementById('cls-log').scrollTop=999999;
      if(!d.running){
        clearInterval(clsPollTimer);clsPollTimer=null;
        document.getElementById('cls-stop').disabled=true;
        document.getElementById('cls-prog').classList.add('hidden');
        document.getElementById('cls-run').disabled=false;
        if(d.result){
          const r=d.result;
          document.getElementById('cls-count').textContent=
            '完成: '+(r.ok||0)+' 成功, '+(r.fail||0)+' 失败'+
            ((r.skip||0)?(', '+(r.skip)+' 已存在被跳过'):'');
          previewNames=null;lastPlan=null;   // 执行完要重新预览
          if(r.fail&&d.errors){
            const es=Object.entries(d.errors).slice(0,5)
              .map(([k,v])=>k.slice(0,30)+': '+v).join('\n');
            document.getElementById('cls-log').textContent+='\n失败明细:\n'+es+'\n';
          }
        }
      }
    });
  },500);
}

// ---- 统计 ----
let statField=null, statShown=0, statTotal=0, statTimer=null;
const STAT_PAGE=20, STAT_STEP=100;

function loadStats(){
  loadStatScopeHint();
  fetch('/api/stats?scope='+statScope()).then(r=>r.json()).then(d=>{
    const el=document.getElementById('stats-summary');
    if(d.error){el.innerHTML='<p>'+esc(d.error)+'</p>';return;}
    el.innerHTML=
      '<p>压缩包总数: <b>'+d.total+'</b></p>'+
      '<p>有元数据: <b>'+d.with_meta+'</b> / '+d.total+'</p>'+
      '<p>读取失败: <b>'+d.errors+'</b></p>'+
      '<p>平均每个作品 <b>'+d.avg_tags+'</b> 个标签</p>';
    // 字段按钮
    const fe=document.getElementById('stats-fields');
    fe.innerHTML=(d.fields||[]).map(f=>
      '<button class="outline" onclick="pickStatField(\''+f.field+'\')">'+
      esc(FIELD_CN_STAT[f.field]||f.field)+
      ' <span style="font-size:11px;color:#888">'+f.distinct+'</span></button>').join('');
    // 范围里一个作品都没有: 字段按钮会全空, 得说清楚为什么
    if(!(d.fields||[]).length){
      fe.innerHTML=statScope()==='scan'
        ? '<p style="font-size:12px;color:#888">本次扫描范围里没有带元数据的作品'+
          '(还没扫描过的话, 先到「扫描」页跑一次)</p>'
        : '<p style="font-size:12px;color:#888">库里还没有带元数据的作品</p>';
    }
    // 范围变了, 已展开的分布属于旧范围 —— 按新范围重拉一遍;
    // 该字段在新范围里已没有值, 就把明细收起来, 别留个空壳
    if(statField){
      if((d.fields||[]).some(f=>f.field===statField))loadStatField(true);
      else{
        statField=null;
        closeStatDrill();
        document.getElementById('stats-detail-card').style.display='none';
      }
    }
  });
}

function statScope(){return document.getElementById('stats-scope').value}
function onStatScopeChange(){
  closeStatDrill();
  loadStats();
}
function loadStatScopeHint(){
  fetch('/api/scope').then(r=>r.json()).then(d=>{
    const el=document.getElementById('stats-scope-hint');
    if(statScope()==='all'){
      el.textContent=d.scan_count!=null
        ? '(全库; 上次扫描到 '+d.scan_count+' 个)' : '(全库)';
    }else if(d.scan_count==null){
      el.textContent='(还没扫描过, 先到「扫描」页跑一次)';
    }else{
      el.textContent='(上次扫描 '+d.at+' 扫到 '+d.scan_count+' 个)';
    }
  });
}

function pickStatField(f){
  statField=f;statShown=0;
  closeStatDrill();
  document.getElementById('stats-detail-card').style.display='block';
  document.getElementById('stats-q').value='';
  loadStatField(true);
  document.getElementById('stats-detail-card').scrollIntoView({behavior:'smooth'});
}

function onStatSearch(){
  if(statTimer)clearTimeout(statTimer);
  statTimer=setTimeout(()=>loadStatField(true),300);   // 防抖
}

function loadStatField(reset){
  if(!statField)return;
  if(reset)statShown=0;
  const q=document.getElementById('stats-q').value.trim();
  const sort=document.getElementById('stats-sort').value;
  const limit=reset?STAT_PAGE:STAT_STEP;
  fetch('/api/stat_field?'+new URLSearchParams({field:statField,limit:limit,
    offset:statShown,q:q,sort:sort,scope:statScope()}))
    .then(r=>r.json()).then(d=>{
      if(d.error){document.getElementById('stats-bars').innerHTML='<p>'+esc(d.error)+'</p>';return;}
      document.getElementById('stats-detail-title').textContent=
        (FIELD_CN_STAT[statField]||statField)+' 分布';
      const bars=document.getElementById('stats-bars');
      const html=d.top.map(x=>bar(x.name,x.count,d.top[0].count)).join('');
      if(reset){bars.innerHTML=html||'<p style="color:#888">没有匹配的值</p>';}
      else{bars.insertAdjacentHTML('beforeend',html);}
      statShown=d.shown;statTotal=d.total_values;
      // 长尾折叠提示
      let meta=d.total_values+' 个不同值';
      if(d.tail_values>0){
        meta+=' · 还有 '+d.tail_values+' 个未显示(共 '+d.tail_entries+' 条目)';
      }
      document.getElementById('stats-detail-meta').textContent=meta;
      const more=document.getElementById('stats-more');
      const all=document.getElementById('stats-all');
      const hasMore=statShown<d.total_values;
      more.style.display=hasMore?'inline-block':'none';
      all.style.display=hasMore?'inline-block':'none';
    });
}

function statLoadMore(){loadStatField(false);}
function statLoadAll(){
  if(!statField)return;
  const q=document.getElementById('stats-q').value.trim();
  const sort=document.getElementById('stats-sort').value;
  if(statTotal>2000&&!confirm('该字段有 '+statTotal+' 个值,全部加载可能很慢。继续?'))return;
  fetch('/api/stat_field?'+new URLSearchParams({field:statField,limit:2000,
    offset:0,q:q,sort:sort,scope:statScope()}))
    .then(r=>r.json()).then(d=>{
      if(d.error)return;
      const max=d.top.length?d.top[0].count:1;
      document.getElementById('stats-bars').innerHTML=
        d.top.map(x=>bar(x.name,x.count,max)).join('');
      statShown=d.shown;
      let meta=d.total_values+' 个不同值';
      if(d.tail_values>0)meta+=' · 还有 '+d.tail_values+' 个未显示(超过2000上限)';
      document.getElementById('stats-detail-meta').textContent=meta;
      document.getElementById('stats-more').style.display='none';
      document.getElementById('stats-all').style.display='none';
    });
}

function bar(name,count,max){
  const w=max?Math.max(count/max*100,2):2;   // 最小 2% 保证可见
  return '<div class="bar" onclick="statToBrowse(\''+esc(name).replace(/'/g,"")+'\')"'+
    ' style="cursor:pointer"><div class="bar-fill" style="width:'+w+'%">'+
    esc(name)+' <span style="opacity:.8">'+count+'</span></div></div>';
}

// 点分布条 -> 在统计页内展开该值的作品列表(不跳页, 可随时返回)
let statDrill=null;   // 当前下钻的值
function statToBrowse(val){
  if(!statField)return;
  statDrill=val;
  const box=document.getElementById('stats-drill');
  box.style.display='block';
  document.getElementById('stats-drill-title').textContent=
    (FIELD_CN_STAT[statField]||statField)+' = '+val;
  document.getElementById('stats-drill-body').innerHTML='<p>加载中…</p>';
  // 下钻必须跟上面那条分布条用同一个范围: 分布条只算本次扫描到的,
  // 下钻却查全库的话, 点开来的条数和条上写的数对不上。
  fetch('/api/query?'+new URLSearchParams({field:statField,value:val,
                                           contains:'',scope:statScope()}))
    .then(r=>r.json()).then(d=>{
      if(d.error){document.getElementById('stats-drill-body').innerHTML='<p>'+esc(d.error)+'</p>';return;}
      const rows=d.rows.map(r=>{
        const m=cols2obj(d.columns,r);
        return '<tr onclick="showDetail('+JSON.stringify(m.name)+')">'+
          '<td>'+esc(m.name||'')+'</td>'+
          '<td>'+esc(m.artist||'')+'</td>'+
          '<td>'+esc(m.language||'')+'</td>'+
          '<td>'+(m.page_count||'')+'</td></tr>';
      }).join('');
      document.getElementById('stats-drill-body').innerHTML=
        '<div style="font-size:12px;color:#666;margin-bottom:6px">'+
        (d.truncated ? d.count+' / 共 '+d.total+' 个作品(只显示前 '+d.limit+' 个)'
                     : d.count+' 个作品')+'</div>'+
        '<div style="max-height:320px;overflow:auto"><table>'+
        '<thead><tr><th>文件名</th><th>作者</th><th>语言</th><th>页数</th></tr></thead>'+
        '<tbody>'+(rows||'<tr><td colspan=4>无</td></tr>')+'</tbody></table></div>';
      box.scrollIntoView({behavior:'smooth'});
    });
}
function closeStatDrill(){
  statDrill=null;
  document.getElementById('stats-drill').style.display='none';
}

document.getElementById('rd-stage').addEventListener('scroll',rdOnScroll,{passive:true});
showPage('read');

// ---- 工具 ----
function esc(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;')}
function fmtSize(n){if(!n)return'';for(const u of['B','KB','MB','GB']){if(n<1024)return n.toFixed(0)+u;n/=1024}return n.toFixed(1)+'TB'}
const cnMap={name:'文件名',path:'路径',size:'大小',image_count:'图片数',has_meta:'有元数据',error:'错误',title:'标题',title_original:'原标题',artist:'作者',group_name:'社团',parody:'原作',character:'角色',tags:'标签',category:'分类',language:'语言',translated:'翻译',page_count:'页数',source_url:'来源链接',source_gid:'来源ID',year:'年',month:'月',day:'日',format:'格式',scanned_at:'扫描时间',entry_count:'条目数',other_fields:'其它字段'};
</script>
</body>
</html>
"""

VALID_FIELDS = set(scanner.META_COLUMNS) | {"language", "category"}


def _attr_escape(s):
    """把值安全放进 HTML 属性(路径里可能有 & " < 等字符)。"""
    return (str(s).replace("&", "&amp;").replace('"', "&quot;")
            .replace("<", "&lt;").replace(">", "&gt;"))


class Handler(BaseHTTPRequestHandler):
    def _json(self, data, code=200):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        write_api_log("RESP_JSON", {"path": self.path, "code": code,
                                    "bytes": len(body),
                                    "preview": body[:300].decode("utf-8", "replace")})
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _html(self, html):
        # 页面里的路径占位符换成实际值(DB 路径随运行位置变)
        html = html.replace("__DB_PATH__", _attr_escape(DB_PATH))
        html = html.replace("__THUMB_PORT__", str(THUMB_PORT))
        body = html.encode("utf-8")
        write_api_log("RESP_HTML", {"path": self.path, "bytes": len(body)})
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store, must-revalidate")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _image(self, data, mime, cache=False):
        write_api_log("RESP_IMAGE", {"path": self.path, "mime": mime,
                                     "bytes": len(data)})
        self.send_response(200)
        self.send_header("Content-Type", mime)
        self.send_header("Content-Length", str(len(data)))
        if cache:
            self.send_header("Cache-Control", "public, max-age=86400")
        else:
            self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def _read_body(self):
        length = int(self.headers.get("Content-Length", 0))
        if length:
            return json.loads(self.rfile.read(length))
        return {}

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)
        write_api_log("REQ_GET", {"path": path, "query": self.path[:1000]})

        if path == "/" or path == "/index.html":
            self._html(HTML_PAGE)
            return
        if path == "/api/distinct":
            field = qs.get("field", [""])[0]
            scope = qs.get("scope", ["scan"])[0]
            if field not in VALID_FIELDS:
                self._json({"error": f"非法字段: {field}"})
                return
            self._json(API.distinct(field, scope))
            return
        if path == "/api/query":
            field = qs.get("field", [""])[0]
            value = qs.get("value", [""])[0]
            contains = qs.get("contains", [""])[0]
            scope = qs.get("scope", ["scan"])[0]
            if field and field not in VALID_FIELDS:
                self._json({"error": f"非法字段: {field}"})
                return
            self._json(API.query(field, value, contains, scope=scope))
            return
        if path == "/api/cache/stats":
            try:
                self._json(reader.cache_stats(
                    int(qs.get("max_mb", [str(reader.get_cache_max())])[0])))
            except Exception as ex:
                self._json({"error": str(ex)})
            return
        if path == "/api/scope":
            n = LAST_SCAN["names"]
            self._json({"scan_count": (len(n) if n is not None else None),
                        "dirs": LAST_SCAN["dirs"], "at": LAST_SCAN["at"]})
            return
        if path == "/api/detail":
            name = qs.get("name", [""])[0]
            self._json(API.detail(name))
            return
        if path == "/api/upload/status":
            self._json(API.upload_status(qs.get("id", [""])[0]))
            return
        if path == "/api/scan/current":
            active = [sid for sid, w in SCAN_WORKERS.items() if w.get("running")]
            self._json({"scan_id": active[-1] if active else None,
                        "active": bool(active)})
            return
        if path == "/api/scan/status":
            sid = qs.get("id", [""])[0]
            self._json(API.scan_status(sid))
            return
        if path == "/api/scan/stop":
            sid = qs.get("id", [""])[0]
            result = API.scan_stop(sid)
            write_op_log("用户停止扫描", {"scan_id": sid, "result": result})
            self._json(result)
            return
        if path == "/api/classify/status":
            cid = qs.get("id", [""])[0]
            self._json(API.classify_status(cid))
            return
        if path == "/api/classify/stop":
            cid = qs.get("id", [""])[0]
            self._json(API.classify_stop(cid))
            return
        if path == "/api/stats":
            scope = qs.get("scope", ["scan"])[0]
            self._json(API.stats(scope))
            return
        if path == "/api/stat_field":
            field = qs.get("field", [""])[0]
            try:
                limit = min(int(qs.get("limit", ["20"])[0]), 500)
                offset = max(int(qs.get("offset", ["0"])[0]), 0)
            except ValueError:
                self._json({"error": "limit/offset 必须是数字"})
                return
            q = qs.get("q", [""])[0]
            sort = qs.get("sort", ["count"])[0]
            scope = qs.get("scope", ["scan"])[0]
            self._json(API.stat_field(field, limit, offset, q, sort, scope))
            return
        if path == "/api/plugins":
            self._json(API.plugins())
            return
        if path == "/api/library_candidates":
            src_dir = qs.get("src_dir", [""])[0]
            q = qs.get("q", [""])[0]
            self._json(API.library_candidates(src_dir, q))
            return
        if path == "/api/reader/pages":
            name = qs.get("name", [""])[0]
            src_dir = qs.get("src_dir", [""])[0]
            self._json(API.reader_pages(name, src_dir,
                                        qs.get("size", ["0"])[0]))
            return
        if path == "/api/reader/thumb":
            name = qs.get("name", [""])[0]
            src_dir = qs.get("src_dir", [""])[0]
            size = qs.get("size", ["0"])[0]
            got = THUMB_POOL.submit(API.reader_thumb, name, size, src_dir).result(timeout=40)
            if not got:
                self.send_error(404)
                return
            self._image(got[0], got[1], cache=True)
            return
        if path == "/api/reader/page":
            name = qs.get("name", [""])[0]
            src_dir = qs.get("src_dir", [""])[0]
            entry = qs.get("entry", [""])[0]
            got = API.reader_page_image(name, src_dir, entry,
                                        qs.get("size", ["0"])[0])
            if not got:
                self.send_error(404)
                return
            self._image(got[0], got[1], cache=False)
            return
        self.send_error(404)

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path
        try:
            body = self._read_body()
        except Exception as e:
            write_api_log("REQ_POST_BAD", {"path": path, "error": str(e)})
            self._json({"error": f"请求体解析失败: {e}"}, 400)
            return
        write_api_log("REQ_POST", {"path": path, "body": body})

        if path == "/api/client/log":
            flush_client_logs()
            info = body.get("data") if isinstance(body.get("data"), dict) else {}
            write_api_log("CLIENT", {"stage": body.get("stage"),
                                     "t": body.get("t"), "data": info})
            self._json({"ok": True})
            return
        if path == "/api/scan":
            dirs = [d.strip() for d in body.get("dirs", "").split(",") if d.strip()]
            db = body.get("db", DB_PATH)
            rescan = body.get("rescan", False)
            recursive = body.get("recursive", False)   # 界面默认不递归
            if not dirs:
                self._json({"error": "请填目录"})
                return
            result = API.scan_start(dirs, db, rescan, recursive)
            write_op_log("开始扫描", {"scan_id": result.get("scan_id"),
                                    "dirs": dirs, "rescan": rescan,
                                    "recursive": recursive})
            self._json(result)
            return
        if path == "/api/upload/prepare":
            result = API.upload_prepare(
                body.get("local_dir", ""), bool(body.get("recursive", False)),
                body.get("target_root", ""), body.get("compare_root", ""))
            self._json(result)
            return
        if path == "/api/upload/start":
            result = API.upload_start(body.get("plan_id", ""))
            self._json(result)
            return
        if path == "/api/upload/delete":
            paths = body.get("paths", [])
            if not isinstance(paths, list) or not paths:
                self._json({"error": "没有要删除的文件"})
                return
            ok, fail, results = 0, 0, []
            for full in paths:
                full = str(full).strip()
                if not full:
                    continue
                try:
                    os.remove(full)
                    ok += 1
                    results.append((full, "ok", "已删除"))
                except Exception as ex:
                    fail += 1
                    results.append((full, "fail", str(ex)[:200]))
            write_op_log("确认删除上传重复文件", {"ok": ok, "fail": fail,
                                          "paths": paths})
            self._json({"ok": ok, "fail": fail, "results": results})
            return
        if path == "/api/duplicates/delete":
            paths = body.get("paths", [])
            if not isinstance(paths, list) or not paths:
                self._json({"error": "没有要删除的文件"})
                return
            ok, fail, results = 0, 0, []
            for full in paths:
                full = str(full).strip()
                if not full:
                    continue
                try:
                    if src_layer.is_remote(full):
                        root, rel = os.path.split(full)
                        root = root.rstrip("/")
                        if not root or not rel:
                            fail += 1
                            results.append((full, "fail", "路径错误"))
                            continue
                        s = src_layer.get_source(root)
                        ret, msg = s.delete(rel)
                        if ret == "ok":
                            ok += 1
                            results.append((full, "ok", msg))
                        else:
                            fail += 1
                            results.append((full, "fail", msg))
                    else:
                        os.remove(full)
                        ok += 1
                        results.append((full, "ok", "已删除"))
                except Exception as ex:
                    fail += 1
                    results.append((full, "fail", str(ex)[:200]))
            write_op_log("确认删除重复文件", {"ok": ok, "fail": fail,
                                       "paths": paths})
            self._json({"ok": ok, "fail": fail, "results": results})
            return
        if path == "/api/classify":
            write_op_log("开始分类整理", {"names_count": len(body.get("names", []))})
            names = body.get("names", [])
            dest = body.get("dest", "")
            mode = body.get("mode", "copy")
            group_by = body.get("group_by", "")
            src_dir = body.get("src_dir") or None
            recursive = body.get("recursive", False)
            conditions = body.get("conditions", [])
            join = body.get("join", "and")
            group_dir = body.get("group_dir", False)
            if not dest:
                self._json({"error": "缺少目标目录"})
                return
            if not group_by:
                self._json({"error": "请选择按哪个字段建子目录"})
                return
            self._json(API.classify_go(names, dest, mode, group_by,
                                       src_dir, recursive, conditions, join,
                                       group_dir))
            return
        if path == "/api/preview":
            conditions = body.get("conditions", [])
            join = body.get("join", "and")
            base_dir = body.get("base_dir", "")
            group_by = body.get("group_by", "")
            src_dir = body.get("src_dir") or None
            recursive = body.get("recursive", False)
            group_dir = body.get("group_dir", False)
            if not base_dir:
                self._json({"error": "请填目标基础目录"})
                return
            if not group_by:
                self._json({"error": "请选择按哪个字段建子目录"})
                return
            self._json(API.filter_preview(conditions, join, base_dir,
                                          group_by, src_dir, recursive,
                                          group_dir=group_dir))
            return
        if path == "/api/reader/list":
            self._json(API.reader_list(body.get("src_dir", ""),
                                       body.get("conditions", []),
                                       body.get("join", "and"),
                                       body.get("scope", "scan"),
                                       body.get("limit", 200),
                                       force=bool(body.get("force")),
                                       subdirs=bool(body.get("subdirs"))))
            return
        if path == "/api/cache/max":
            try:
                mb = reader.set_cache_max(body.get("max_mb"))
                write_op_log("设置阅读缓存上限", {"max_mb": mb})
                self._json({"ok": True, "max_mb": mb})
            except Exception as ex:
                write_op_log("设置阅读缓存上限失败", {"error": str(ex)})
                self._json({"error": str(ex)})
            return
        if path == "/api/cache/clear":
            try:
                n = reader.clear_cache()
                write_op_log("清空阅读缓存", {"removed": n})
                self._json({"ok": True, "removed": n})
            except Exception as ex:
                write_op_log("清空阅读缓存失败", {"error": str(ex)})
                self._json({"error": str(ex)})
            return
        if path == "/api/source":
            src_dir = body.get("src_dir", "")
            recursive = body.get("recursive", False)
            if not src_dir:
                self._json({"error": "请填源目录"})
                return
            self._json(API.source_list(src_dir, recursive))
            return
        self.send_error(404)

    def send_error(self, code, message=None, explain=None):
        write_api_log("HTTP_ERROR", {"path": self.path, "code": code,
                                     "message": str(message or "")})
        return super().send_error(code, message, explain)

    def log_message(self, *args):
        try:
            write_api_log("HTTP_SERVER", {"message": " ".join(str(a) for a in args)})
        except Exception:
            pass


def main():
    write_op_log("服务启动", {"pid": os.getpid(), "port": PORT, "db": DB_PATH})
    print(f"漫画管理 APP")
    print(f"数据库: {DB_PATH}")
    print(f"浏览器打开: http://127.0.0.1:{PORT}")
    print(f"Ctrl+C 停止\n")
    class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
        daemon_threads = True
    server = ThreadedHTTPServer(("0.0.0.0", PORT), Handler)
    thumb_server = ThreadedHTTPServer(("0.0.0.0", THUMB_PORT), Handler)
    threading.Thread(target=thumb_server.serve_forever,
                     name="thumb-http-server", daemon=True).start()
    print(f"缩略图独立端口: {THUMB_PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n已停止")
        server.shutdown()
        thumb_server.shutdown()


if __name__ == "__main__":
    main()

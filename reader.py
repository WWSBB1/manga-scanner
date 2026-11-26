#!/usr/bin/env python3
"""阅读器: 从 zip 取图 + 缩略图缓存。

缩略图存在项目目录下独立的 SQLite 库里(不是散落的图片文件):
- 不会出现在 Android 相册/媒体扫描里
- 按文件名索引: 同名漫画无论 size 或所在子目录变化, 都复用同一张缩略图
- 每个缩略图压到几十 KB, 单条 BLOB 直接由 HTTP 返回
"""

import base64
import io
import json
import logging
import os
import queue
import re
import sqlite3
import threading
import time
import zipfile

from PIL import Image, ImageOps

CACHE_DB = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "thumbs.db")
THUMB_W = 400
THUMB_H = 560
JPEG_QUALITY = 72
LOCK = threading.Lock()
LOCAL_TASK_QUEUE = queue.Queue()
REMOTE_TASK_QUEUE = queue.Queue()

# 远程 zip 会话缓存: key=(root, name), value={"fp":BytesIO,"zf":ZipFile,"names":[...]}
# 只读一次中央目录, 之后翻页按 offset 直接内存读取。
MANGA_SESSIONS = {}
MANGA_SESSION_LOCK = threading.Lock()
MANGA_SESSION_MAX = 4

# 远程漫画磁盘缓存: 整包缓存, 总大小上限可配置, LRU 清理。
CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "manga_cache")
CACHE_DB_FILE = os.path.join(CACHE_DIR, "index.db")
CACHE_CONFIG_FILE = os.path.join(CACHE_DIR, "config.json")
CACHE_DEFAULT_MAX_MB = 500
CACHE_LOCK = threading.Lock()


def _clamp_max_mb(value):
    try:
        n = int(value)
    except (TypeError, ValueError):
        return CACHE_DEFAULT_MAX_MB
    return max(50, min(5000, n))


def _read_max_mb():
    try:
        with open(CACHE_CONFIG_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return _clamp_max_mb(data.get("max_mb"))
    except Exception:
        return CACHE_DEFAULT_MAX_MB


def _write_max_mb(value):
    n = _clamp_max_mb(value)
    os.makedirs(CACHE_DIR, exist_ok=True)
    tmp = CACHE_CONFIG_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"max_mb": n}, f)
    os.replace(tmp, CACHE_CONFIG_FILE)
    return n


def set_cache_max(max_mb):
    """保存页面设置的上限; 后端缓存淘汰实际使用这个值。"""
    n = _write_max_mb(max_mb)
    logging.getLogger("reader").info("MANGA_CACHE_MAX_SET max_mb=%s", n)
    return n


def get_cache_max():
    return _read_max_mb()


def _cache_init():
    os.makedirs(CACHE_DIR, exist_ok=True)
    conn = sqlite3.connect(CACHE_DB_FILE, timeout=30)
    conn.execute("""CREATE TABLE IF NOT EXISTS files (
        key TEXT PRIMARY KEY,
        path TEXT NOT NULL,
        size INTEGER NOT NULL,
        last_used REAL NOT NULL
    )""")
    conn.commit(); conn.close()


def _cache_key(root, name):
    """缓存索引 key: 直接用源文件名(basename), 不带相对路径, 不用哈希, 不用大小。"""
    return (name or "").replace("\\", "/").rstrip("/").rsplit("/", 1)[-1]


_INVALID_CACHE_FILENAME = re.compile(r'[<>:"/\\|?*\x00-\x1f]')


def _cache_filename(name):
    """磁盘缓存文件名: 只取源文件名, 平铺在 manga_cache 根目录。"""
    base = str(name or "").replace("\\", "/").rstrip("/")
    leaf = os.path.basename(base)
    leaf = _INVALID_CACHE_FILENAME.sub("_", leaf).strip().strip(".")
    if not leaf:
        leaf = "manga.zip"
    elif not leaf.lower().endswith(".zip"):
        leaf += ".zip"
    return leaf

def _unique_cache_path(conn, key, name):
    """生成缓存路径; 同名冲突只加 (2)/(3), 不用哈希。"""
    rel = _cache_filename(name)
    dest = os.path.join(CACHE_DIR, rel)
    taken = conn.execute(
        "SELECT key FROM files WHERE path=? AND key<>?", (dest, key)).fetchone()
    if not taken:
        return dest
    stem, ext = os.path.splitext(rel)
    n = 2
    while True:
        rel_n = f"{stem} ({n}){ext}"
        dest = os.path.join(CACHE_DIR, rel_n)
        taken = conn.execute(
            "SELECT key FROM files WHERE path=? AND key<>?", (dest, key)).fetchone()
        if not taken:
            return dest
        n += 1


def _cache_get(key):
    """命中则更新 last_used 并返回文件路径。"""
    with CACHE_LOCK:
        _cache_init()
        conn = sqlite3.connect(CACHE_DB_FILE, timeout=30)
        try:
            row = conn.execute("SELECT path FROM files WHERE key=?", (key,)).fetchone()
            if row and os.path.isfile(row[0]):
                conn.execute("UPDATE files SET last_used=? WHERE key=?", (time.time(), key))
                conn.commit()
                return row[0]
            if row:
                conn.execute("DELETE FROM files WHERE key=?", (key,))
                conn.commit()
        finally:
            conn.close()
    return None


def _cache_put(key, data, max_mb, name=None):
    """写入缓存文件并登记; 超限按 last_used 从旧到新删除。"""
    with CACHE_LOCK:
        _cache_init()
        conn = sqlite3.connect(CACHE_DB_FILE, timeout=30)
        try:
            old = conn.execute("SELECT path FROM files WHERE key=?", (key,)).fetchone()
            dest = old[0] if old and os.path.isfile(old[0]) else \
                _unique_cache_path(conn, key, name or key)
            tmp = dest + ".part"
            with open(tmp, "wb") as f: f.write(data)
            os.replace(tmp, dest)
            conn.execute("INSERT OR REPLACE INTO files (key,path,size,last_used) VALUES (?,?,?,?)",
                         (key, dest, len(data), time.time()))
            conn.commit()
            total = conn.execute("SELECT COALESCE(SUM(size),0) FROM files").fetchone()[0]
            limit = int(max_mb) * 1024 * 1024
            if total > limit:
                for k, p, sz in conn.execute(
                        "SELECT key,path,size FROM files ORDER BY last_used ASC"):
                    if total <= limit: break
                    try: os.remove(p)
                    except Exception: pass
                    conn.execute("DELETE FROM files WHERE key=?", (k,))
                    total -= sz
            conn.commit()
        finally:
            conn.close()
    return dest


def cache_stats(max_mb=None):
    """返回缓存统计: 文件数、总字节、上限。"""
    limit_mb = _clamp_max_mb(max_mb if max_mb is not None else _read_max_mb())
    with CACHE_LOCK:
        _cache_init()
        conn = sqlite3.connect(CACHE_DB_FILE, timeout=30)
        try:
            n, total = conn.execute(
                "SELECT COUNT(*), COALESCE(SUM(size),0) FROM files").fetchone()
            return {"count": n, "bytes": total, "max_mb": limit_mb}
        finally:
            conn.close()


def clear_cache():
    logging.getLogger("reader").info("MANGA_CACHE_CLEAR_BEGIN")
    with CACHE_LOCK:
        _cache_init()
        conn = sqlite3.connect(CACHE_DB_FILE, timeout=30)
        removed = 0
        try:
            for (p,) in conn.execute("SELECT path FROM files"):
                try:
                    os.remove(p)
                    removed += 1
                except Exception:
                    pass
            conn.execute("DELETE FROM files"); conn.commit()
        finally:
            conn.close()
    logging.getLogger("reader").info("MANGA_CACHE_CLEAR_DONE removed=%s", removed)
    return removed

IMAGE_EXT = (".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp")
_NUM = re.compile(r"(\d+)")


def _natural_key(name):
    parts = _NUM.split(name)
    return tuple((1, int(p)) if p.isdigit() else (0, p.lower())
                 for p in parts)


def _thumb_name(name):
    """缩略图只按文件名索引, 去掉可能带上的子目录前缀。"""
    return name.replace("\\", "/").rstrip("/").rsplit("/", 1)[-1]


def _connect():
    conn = sqlite3.connect(CACHE_DB, timeout=30)
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("""CREATE TABLE IF NOT EXISTS thumbs (
        name TEXT PRIMARY KEY,
        size INTEGER NOT NULL,
        entry TEXT NOT NULL,
        mime TEXT NOT NULL,
        data BLOB NOT NULL,
        created REAL NOT NULL
    )""")
    _migrate_thumbs(conn)
    return conn


def _migrate_thumbs(conn):
    """旧库按 (name,size) 存, 迁移成按文件名唯一。

    同名记录保留 created 最新的一条; 带目录前缀的记录折算成文件名。
    size 只作为普通属性保存, 不用来选择/判断文件。
    """
    cols = {r[1] for r in conn.execute("PRAGMA table_info(thumbs)")}
    if "data" not in cols:
        return
    if {"name", "size"}.issubset(cols):
        pk = [r[1] for r in conn.execute("PRAGMA table_info(thumbs)")
              if r[5]]
        if pk == ["name"]:
            return
        rows = conn.execute(
            "SELECT name,size,entry,mime,data,created FROM thumbs").fetchall()
        best = {}
        for name, size, entry, mime, data, created in rows:
            base = _thumb_name(name)
            old = best.get(base)
            if old is None or (created or 0) >= (old[5] or 0):
                best[base] = (base, size or 0, entry, mime, data, created)
        conn.execute("DROP TABLE thumbs")
        conn.execute("""CREATE TABLE thumbs (
            name TEXT PRIMARY KEY,
            size INTEGER NOT NULL,
            entry TEXT NOT NULL,
            mime TEXT NOT NULL,
            data BLOB NOT NULL,
            created REAL NOT NULL
        )""")
        conn.executemany(
            "INSERT INTO thumbs (name,size,entry,mime,data,created) "
            "VALUES (?,?,?,?,?,?)", best.values())
        conn.commit()


def _image_names(z):
    out = []
    for info in z.infolist():
        if info.is_dir():
            continue
        low = info.filename.lower()
        if low.endswith(IMAGE_EXT) and "__MACOSX" not in info.filename \
                and not os.path.basename(info.filename).startswith("."):
            out.append(info.filename)
    return sorted(out, key=_natural_key)


def _zip_index(fp):
    """读一次 zip 中央目录, 返回 (图片页名, {页名:(offset,size)})。"""
    with zipfile.ZipFile(fp) as z:
        names = _image_names(z)
        offsets = {}
        for info in z.infolist():
            if info.filename in names:
                offsets[info.filename] = (info.header_offset, info.compress_size)
    return names, offsets


def zip_entries(fp):
    """从 zip 会话中导出完整 entry 元数据(JSON 可序列化)。"""
    zf = fp["zf"] if isinstance(fp, dict) else None
    if zf is None:
        with zipfile.ZipFile(fp) as z:
            infos = list(z.infolist())
    else:
        infos = list(zf.infolist())
    out = []
    for info in infos:
        low = info.filename.lower()
        if (info.is_dir() or not low.endswith(IMAGE_EXT)
                or "__MACOSX" in info.filename
                or os.path.basename(info.filename).startswith(".")):
            continue
        out.append({
            "p": info.filename, "o": info.header_offset,
            "c": info.compress_size, "f": info.file_size,
            "m": info.compress_type,
            "filename": info.filename,
            "date_time": list(info.date_time),
            "compress_type": info.compress_type,
            "comment": info.comment.decode("utf-8", "replace") if info.comment else "",
            "extra_b64": base64.b64encode(info.extra).decode() if info.extra else "",
            "crc": info.CRC,
            "compress_size": info.compress_size,
            "file_size": info.file_size,
            "flag_bits": info.flag_bits,
            "volume": info.volume,
            "internal_attr": info.internal_attr,
            "external_attr": info.external_attr,
            "header_offset": info.header_offset,
            "create_system": info.create_system,
            "create_version": info.create_version,
            "extract_version": info.extract_version,
            "reserved": info.reserved,
        })
    out.sort(key=lambda x: _natural_key(x["p"]))
    return out


def _list_pages_fp(fp):
    """列出图片页名; 支持本地文件对象和远程会话 dict。"""
    if isinstance(fp, dict):
        return fp["names"]
    try:
        with zipfile.ZipFile(fp) as z:
            return _image_names(z)
    except (OSError, zipfile.BadZipFile):
        return None


def get_page_fp(fp, entry):
    """从可 seek 的文件对象或远程会话读指定页, 返回 (bytes, mime)。"""
    if isinstance(fp, dict):
        try:
            with fp["lock"]:
                data = fp["zf"].read(entry)
        except Exception:
            return None
        return data, _mime_of(entry)
    try:
        with zipfile.ZipFile(fp) as z:
            names = _image_names(z)
            if entry not in names:
                return None
            data = z.read(entry)
    except (OSError, zipfile.BadZipFile, KeyError):
        return None
    low = entry.lower()
    if low.endswith(".png"):
        mime = "image/png"
    elif low.endswith(".webp"):
        mime = "image/webp"
    elif low.endswith(".gif"):
        mime = "image/gif"
    elif low.endswith(".bmp"):
        mime = "image/bmp"
    else:
        mime = "image/jpeg"
    return data, mime


def _mime_of(entry):
    low=entry.lower()
    if low.endswith(".png"): return "image/png"
    if low.endswith(".webp"): return "image/webp"
    if low.endswith(".gif"): return "image/gif"
    if low.endswith(".bmp"): return "image/bmp"
    return "image/jpeg"


def _read_entry_at(fp, offset, size):
    """按本地头偏移读一个 zip entry: 读 30 字节头 + filename/extra + 数据。"""
    import struct
    try:
        fp.seek(offset)
        head = fp.read(30)
        if len(head) < 30 or head[:4] != b"PK\x03\x04":
            return None
        _sig,_ver,_flag,_method,_t,_d,_crc,_cs,_us,fnl,exl = struct.unpack("<IHHHHHIIIHH", head)
        fp.seek(offset + 30 + fnl + exl)
        return fp.read(_cs)
    except Exception:
        return None


def list_pages(path):
    """列出本地 zip 里的图片页名。"""
    if not os.path.isfile(path):
        return None
    return _list_pages_fp(path)


def get_page(path, entry):
    """本地 zip 读页, 返回 (bytes, mime)。"""
    try:
        with zipfile.ZipFile(path) as z:
            names = _image_names(z)
            if entry not in names:
                return None
            data = z.read(entry)
    except (OSError, zipfile.BadZipFile, KeyError):
        return None
    low = entry.lower()
    if low.endswith(".png"):
        mime = "image/png"
    elif low.endswith(".webp"):
        mime = "image/webp"
    elif low.endswith(".gif"):
        mime = "image/gif"
    elif low.endswith(".bmp"):
        mime = "image/bmp"
    else:
        mime = "image/jpeg"
    return data, mime


def _thumb_from(data):
    im = Image.open(io.BytesIO(data))
    try:
        im = ImageOps.exif_transpose(im)
        im.thumbnail((THUMB_W, THUMB_H), Image.Resampling.LANCZOS)
        buf = io.BytesIO()
        im.convert("RGB").save(buf, "JPEG", quality=JPEG_QUALITY,
                               optimize=True, progressive=True)
        return buf.getvalue()
    finally:
        im.close()


def get_thumb(name, size, source=None, root=None, path=None):
    """取缩略图: 按文件名命中缓存; 未命中才从数据源读第一张图并入库。

    name/path 保留完整相对路径给数据源打开; key 只用于本地缩略图缓存。
    """
    key = _thumb_name(name)
    logging.getLogger("reader").info("THUMB_REQUEST name=%s size=%s", key, size)
    with LOCK:
        conn = _connect()
        try:
            row = conn.execute(
                "SELECT data FROM thumbs WHERE name=?", (key,)).fetchone()
            if row:
                logging.getLogger("reader").info("THUMB_HIT name=%s size=%s", key, size)
                return bytes(row[0]), "image/jpeg"
        finally:
            conn.close()
    got = None
    if source is not None:
        try:
            fp = source.open_file(name)
        except Exception:
            return None
        try:
            pages = _list_pages_fp(fp)
            if pages:
                data = get_page_fp(fp, pages[0])
                if data:
                    got = (pages[0], data[0])
        finally:
            try: fp.close()
            except Exception: pass
    elif path:
        pages = _list_pages_fp(path)
        if pages:
            data = get_page(path, pages[0])
            if data:
                got = (pages[0], data[0])
    if not got:
        logging.getLogger("reader").warning("THUMB_MISS_READ name=%s size=%s", key, size)
        return None
    logging.getLogger("reader").info("THUMB_GENERATE name=%s size=%s", key, size)
    entry, data = got
    thumb = _thumb_from(data)
    if not thumb:
        return None
    with LOCK:
        conn = _connect()
        try:
            conn.execute(
                "INSERT OR REPLACE INTO thumbs "
                "(name,size,entry,mime,data,created) VALUES (?,?,?,?,?,?)",
                (key, size, entry, "image/jpeg", thumb, time.time()))
            conn.commit()
        finally:
            conn.close()
    return thumb, "image/jpeg"


def thumb_missing(name, size):
    """缓存里没有这个文件名(忽略 size)的缩略图就返回 True。"""
    name = _thumb_name(name)
    with LOCK:
        conn = _connect()
        try:
            row = conn.execute(
                "SELECT 1 FROM thumbs WHERE name=?", (name,)).fetchone()
            return not row
        finally:
            conn.close()


def thumb_keys():
    """返回已有缩略图文件名集合, 供扫描阶段批量判断。"""
    with LOCK:
        conn = _connect()
        try:
            return set(r[0] for r in conn.execute("SELECT name FROM thumbs"))
        finally:
            conn.close()


def _warm_worker(task_queue):
    """后台缩略图预热(常驻)。本地并发, 远程队列串行避免 WebDAV 限流。"""
    import source as src_layer
    srcs = {}
    while True:
        queued = task_queue.get()
        if isinstance(queued, tuple) and len(queued) == 2:
            (root, name), size = queued
        elif isinstance(queued, tuple) and len(queued) == 3 and \
                isinstance(queued[0], tuple):
            (root, name), size = queued[0], queued[2]
        else:
            root, name, size = queued
        try:
            if root not in srcs:
                srcs[root] = src_layer.get_source(root)
            get_thumb(name, size, source=srcs[root], root=root)
        except Exception:
            pass
        finally:
            with _WARM_PENDING_LOCK:
                _WARM_PENDING.discard((root, name))
            task_queue.task_done()


_WARM_PENDING = set()
_WARM_PENDING_LOCK = threading.Lock()


def thumb_warm(root, items):
    """把缺缓存的缩略图排进后台队列: items = [(name, size), ...]。

    一次 SELECT 取出所有已缓存的文件名, 在内存对比; size 不作为身份判断。
    _WARM_PENDING 防止后台还没生成完时重复排队。
    """
    import source as src_layer
    with LOCK:
        conn = _connect()
        try:
            have = set(r[0] for r in conn.execute("SELECT name FROM thumbs"))
        finally:
            conn.close()
    q = REMOTE_TASK_QUEUE if src_layer.is_remote(root) else LOCAL_TASK_QUEUE
    n = 0
    for name, size in items:
        key = (root, name)
        with _WARM_PENDING_LOCK:
            if key in _WARM_PENDING or name in have:
                continue
            _WARM_PENDING.add(key)
        q.put((key, name, size))
        n += 1
    return n


def open_manga(source, name, size=0, root=""):
    """远程漫画: 一次整包拉到内存(BytesIO), 之后所有页毫秒级读取。

    实测十几 MB 整包 HTTP GET 约 1.3 秒; 逐 Range 每次都要新请求,
    前三页累计会到 10 秒以上。整包方案明显更快。
    """
    if source is None:
        return None
    if hasattr(source, "open_path") and getattr(source, "kind", "") == "webdav-http":
        sz = int(size or 0)
        key = (root, name)
        with MANGA_SESSION_LOCK:
            hit = MANGA_SESSIONS.get(key)
            if hit is None:
                ck = _cache_key(root, name)
                cached = _cache_get(ck)
                if cached:
                    with open(cached, "rb") as f: data = f.read()
                else:
                    fp = source.open_path(name, sz)
                    try: data = fp.read()
                    finally:
                        try: fp.close()
                        except Exception: pass
                    if not data: return None
                    try: _cache_put(ck, data, _read_max_mb(), name)
                    except Exception: pass
                zf = zipfile.ZipFile(io.BytesIO(data))
                names = _image_names(zf)
                hit = {"fp": io.BytesIO(data), "zf": zf, "names": names,
                       "lock": threading.Lock(), "data": data}
                MANGA_SESSIONS[key] = hit
                if len(MANGA_SESSIONS) > MANGA_SESSION_MAX:
                    MANGA_SESSIONS.pop(next(iter(MANGA_SESSIONS)), None)
            return hit
    return source.open_file(name)


def clear_thumbs():
    with LOCK:
        conn = _connect()
        try:
            n = conn.execute("SELECT COUNT(*) FROM thumbs").fetchone()[0]
            conn.execute("DELETE FROM thumbs")
            conn.commit()
            return n
        finally:
            conn.close()


for _i in range(4):
    threading.Thread(target=_warm_worker, args=(LOCAL_TASK_QUEUE,),
                     name="thumb-warm-local", daemon=True).start()
threading.Thread(target=_warm_worker, args=(REMOTE_TASK_QUEUE,),
                 name="thumb-warm-remote", daemon=True).start()

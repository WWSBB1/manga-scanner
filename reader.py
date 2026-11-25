#!/usr/bin/env python3
"""阅读器: 从 zip 取图 + 缩略图缓存。

缩略图存在项目目录下独立的 SQLite 库里(不是散落的图片文件):
- 不会出现在 Android 相册/媒体扫描里
- 按 (文件名, 大小) 索引, 与原库共用的"名字+大小"口径一致
- 每个缩略图压到几十 KB, 单条 BLOB 直接由 HTTP 返回
"""

import base64
import io
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

# 远程漫画整包缓存: key=(root, name, size), value=bytes。
# 实测 10MB 整包约 2 秒, 之后每页本地读 0.01 秒; Range 逐页读第 2/3 页
# 因 WebDAV 限流会到 10 秒以上, 所以远程打开时一次性拉全。
# 远程 zip 会话缓存: key=(root, name, size), value={"fp":RemoteFile,"names":[...],"offsets":...}
# 只读一次中央目录, 之后翻页按 offset 直接 Range 读取, 不下载整包。
MANGA_SESSIONS = {}
MANGA_SESSION_LOCK = threading.Lock()
MANGA_SESSION_MAX = 4

# 远程漫画磁盘缓存: 整包缓存, 总大小默认上限 500MB, LRU 清理。
CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "manga_cache")
CACHE_DB_FILE = os.path.join(CACHE_DIR, "index.db")
CACHE_DEFAULT_MAX_MB = 500
CACHE_LOCK = threading.Lock()


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


def _cache_key(root, name, size):
    import hashlib
    return hashlib.sha1(f"{root}|{name}|{size}".encode()).hexdigest()


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


def _cache_put(key, data, max_mb):
    """写入缓存文件并登记; 超限按 last_used 从旧到新删除。"""
    with CACHE_LOCK:
        _cache_init()
        dest = os.path.join(CACHE_DIR, key + ".zip")
        tmp = dest + ".part"
        with open(tmp, "wb") as f: f.write(data)
        os.replace(tmp, dest)
        conn = sqlite3.connect(CACHE_DB_FILE, timeout=30)
        try:
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


def cache_stats(max_mb):
    """返回缓存统计: 文件数、总字节、上限。"""
    with CACHE_LOCK:
        _cache_init()
        conn = sqlite3.connect(CACHE_DB_FILE, timeout=30)
        try:
            n, total = conn.execute(
                "SELECT COUNT(*), COALESCE(SUM(size),0) FROM files").fetchone()
            return {"count": n, "bytes": total, "max_mb": int(max_mb)}
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


def _connect():
    conn = sqlite3.connect(CACHE_DB, timeout=30)
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("""CREATE TABLE IF NOT EXISTS thumbs (
        name TEXT NOT NULL,
        size INTEGER NOT NULL,
        entry TEXT NOT NULL,
        mime TEXT NOT NULL,
        data BLOB NOT NULL,
        created REAL NOT NULL,
        PRIMARY KEY (name, size)
    )""")
    return conn


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
    logging.getLogger("reader").info("THUMB_REQUEST name=%s size=%s", name, size)
    """取缩略图: 缓存命中直接返回; 未命中才从数据源读第一张图并入库。

    source/root: 数据源抽象(open_file), 本地与远程共用;
    path: 兼容旧的本地文件路径调用。
    """
    with LOCK:
        conn = _connect()
        try:
            row = conn.execute(
                "SELECT data FROM thumbs WHERE name=? AND size=?",
                (name, size)).fetchone()
            if row:
                logging.getLogger("reader").info("THUMB_HIT name=%s size=%s", name, size)
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
        logging.getLogger("reader").warning("THUMB_MISS_READ name=%s size=%s", name, size)
        return None
    logging.getLogger("reader").info("THUMB_GENERATE name=%s size=%s", name, size)
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
                (name, size, entry, "image/jpeg", thumb, time.time()))
            conn.commit()
        finally:
            conn.close()
    return thumb, "image/jpeg"


def thumb_missing(name, size):
    """缓存里没有这个 (文件名, 大小) 的缩略图就返回 True。"""
    with LOCK:
        conn = _connect()
        try:
            row = conn.execute(
                "SELECT 1 FROM thumbs WHERE name=? AND size=?",
                (name, size)).fetchone()
            return not row
        finally:
            conn.close()


def thumb_keys():
    """返回已有缩略图 (name,size) 集合, 供扫描阶段批量判断。"""
    with LOCK:
        conn = _connect()
        try:
            return set((r[0], r[1]) for r in conn.execute(
                "SELECT name,size FROM thumbs"))
        finally:
            conn.close()


def _warm_worker(task_queue):
    """后台缩略图预热(常驻)。本地并发, 远程队列串行避免 WebDAV 限流。"""
    import source as src_layer
    srcs = {}
    while True:
        root, name, size = task_queue.get()
        try:
            if root not in srcs:
                srcs[root] = src_layer.get_source(root)
            get_thumb(name, size, source=srcs[root], root=root)
        except Exception:
            pass
        finally:
            with _WARM_PENDING_LOCK:
                _WARM_PENDING.discard((root, name, size))
            task_queue.task_done()


_WARM_PENDING = set()
_WARM_PENDING_LOCK = threading.Lock()


def thumb_warm(root, items):
    """把缺缓存的缩略图排进后台队列: items = [(name, size), ...]。

    一次 SELECT 取出所有已缓存的 (name,size) 在内存对比, 逐本查库会拖慢列表。
    _WARM_PENDING 防止后台还没生成完时重复排队。
    """
    import source as src_layer
    with LOCK:
        conn = _connect()
        try:
            have = set((r[0], r[1]) for r in conn.execute(
                "SELECT name, size FROM thumbs"))
        finally:
            conn.close()
    q = REMOTE_TASK_QUEUE if src_layer.is_remote(root) else LOCAL_TASK_QUEUE
    n = 0
    for name, size in items:
        key = (root, name, size)
        with _WARM_PENDING_LOCK:
            if key in _WARM_PENDING or (name, size) in have:
                continue
            _WARM_PENDING.add(key)
        q.put(key)
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
        key = (root, name, sz)
        with MANGA_SESSION_LOCK:
            hit = MANGA_SESSIONS.get(key)
            if hit is None:
                ck = _cache_key(root, name, sz)
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
                    try: _cache_put(ck, data, CACHE_DEFAULT_MAX_MB)
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

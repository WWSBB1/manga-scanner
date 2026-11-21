#!/usr/bin/env python3
"""阅读器: 从本地 zip 直接取图 + 缩略图缓存。

缩略图存在项目目录下独立的 SQLite 库里(不是散落的图片文件):
- 不会出现在 Android 相册/媒体扫描里
- 按 (文件名, 大小) 索引, 与原库共用的"名字+大小"口径一致
- 每个缩略图压到几十 KB, 单条 BLOB 直接由 HTTP 返回
"""

import io
import os
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

import re

IMAGE_EXT = (".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp")

# 文件名按数字段拆开: 001.jpg -> ['', 1, '.jpg'], page10 -> ['page', 10, '']
_NUM = re.compile(r"(\d+)")


def _natural_key(name):
    parts = _NUM.split(name)
    # 文本段按小写比较, 数字段按数值比较; 末尾补类型标记让短的名字排前面
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


def list_pages(path):
    """列出 zip 里的图片页名。只做本地文件。"""
    if not os.path.isfile(path):
        return None
    try:
        with zipfile.ZipFile(path) as z:
            return _image_names(z)
    except (OSError, zipfile.BadZipFile):
        return None


def get_page(path, entry):
    """返回 (bytes, mime)。entry 必须来自 list_pages, 不接受任意路径。"""
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
        # 有些扫描件是 CMYK / P 模式, JPEG 编码前统一转 RGB
        if im.mode not in ("RGB", "L"):
            im = im.convert("RGB")
        im = ImageOps.exif_transpose(im)
        im.thumbnail((THUMB_W, THUMB_H), Image.Resampling.LANCZOS)
        buf = io.BytesIO()
        im.convert("RGB").save(buf, "JPEG", quality=JPEG_QUALITY,
                               optimize=True, progressive=True)
        return buf.getvalue()
    finally:
        im.close()


def get_thumb(name, size, path):
    """取缩略图; 命中缓存直接返回, 否则从第一张图片生成并入库。"""
    with LOCK:
        conn = _connect()
        try:
            row = conn.execute(
                "SELECT data FROM thumbs WHERE name=? AND size=?",
                (name, size)).fetchone()
            if row:
                return bytes(row[0]), "image/jpeg"
        finally:
            conn.close()
    pages = list_pages(path)
    if not pages:
        return None
    got = get_page(path, pages[0])
    if not got:
        return None
    thumb = _thumb_from(got[0])
    if not thumb:
        return None
    with LOCK:
        conn = _connect()
        try:
            conn.execute(
                "INSERT OR REPLACE INTO thumbs "
                "(name,size,entry,mime,data,created) VALUES (?,?,?,?,?,?)",
                (name, size, pages[0], got[1], thumb, time.time()))
            conn.commit()
        finally:
            conn.close()
    return thumb, "image/jpeg"


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

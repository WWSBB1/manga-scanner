#!/usr/bin/env python3
"""漫画压缩包元数据采集器(本地文件版)

扫描本地目录下的漫画压缩包(.zip/.cbz),解析包内元数据文件的所有字段,
归一化后存入 SQLite。表以文件名作唯一键(增量判断用)。

已支持的元数据文件(实测四种都出现过):
    ComicInfo.xml   ComicRack 标准,nhentai-helper 产出
    info.json       nhentai-helper 产出,gallery_info 结构
    meta.json       comic-looms 产出
    metadata.json   其它下载器,标签列表在顶层

增量规则:
    - 上次成功且文件名已存在 -> 跳过(不比 size、不比 mtime)
    - 读过但没解析出元数据 -> 也跳过(重读结果不会变)
    - 上次报错 -> 下次重试
    - 同名不同目录 -> 告警跳过,不覆盖

本模块同时是 GUI 的后端库: scan_dirs() / list_archives() / filter_archives()
都可直接 import 使用。命令行入口: python3 scanner.py [--dir ...] [--db ...]
"""

import argparse
import json
import logging
import os
import sqlite3
import sys
import time
import xml.etree.ElementTree as ET
import zipfile
from concurrent.futures import ThreadPoolExecutor

# 数据源层: 本地磁盘与 WebDAV 远程共用一套接口。source.py 不反向依赖
# 本模块, 所以不是循环导入。
import source as _source

# ---------------------------------------------------------------- 配置

DEFAULT_SCAN_DIRS = ["/storage/emulated/0/Download"]
# 默认库放在项目目录里(和程序文件一起, 便于整体移动/备份)
DEFAULT_DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "manga.db")

ARCHIVE_EXT = (".zip", ".cbz")
SUPPORTED = ARCHIVE_EXT          # 能真正解析的
NOTICE_EXT = (".rar", ".7z")     # 只报告、不解析
SUPPORTED_NOTE = "rar/7z 需要第三方库,只统计不解析"

META_FILE_COLUMNS = {
    "comicinfo.xml": "comicinfo_xml",
    "info.json": "info_json",
    "meta.json": "meta_json",
    "metadata.json": "metadata_json",
}
GALLERY_PRIORITY = ["info_json", "meta_json", "metadata_json"]

OTHER_META_EXT = (".json", ".xml", ".txt")
IMAGE_EXT = (".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".avif")

LANG_MAP = {
    "zh": "chinese", "zh-cn": "chinese", "zh-tw": "chinese",
    "ja": "japanese", "jp": "japanese",
    "en": "english", "ko": "korean",
    "chinese": "chinese", "japanese": "japanese", "english": "english",
}

# 归一化进 meta 表的 ComicInfo 字段,其余进 other_fields
CI_MAPPED = {"Title", "Writer", "Publisher", "Series", "Tags", "Format",
             "LanguageISO", "PageCount", "Web", "Year", "Month", "Day",
             "Translated"}

# 下载器对"未知"字段填的字面量,一律当空处理,不当作真实值入库
PLACEHOLDERS = {"unknown", "n/a", "none", "null"}


# ---------------------------------------------------------------- 小工具

def _is_placeholder(v):
    """是否是占位符(Nhentai Helper 等填的 Unknown)。"""
    return str(v).strip().lower() in PLACEHOLDERS


def _first(*vals):
    for v in vals:
        if v not in (None, "", [], {}):
            return v
    return None


def _join(v):
    if v is None:
        return None
    if isinstance(v, (list, tuple)):
        items = [str(x).strip() for x in v
                 if str(x).strip() and not _is_placeholder(x)]
        return ", ".join(items) or None
    s = str(v).strip()
    if not s or _is_placeholder(s):
        return None
    return s


def _int(v):
    try:
        return int(str(v).strip())
    except (TypeError, ValueError):
        return None


def _decode(data):
    for enc in ("utf-8", "gbk", "cp437"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", "replace")


# 各字段自己的"等于没填"取值。original 是"原创作品"(不是同人),
# 对"原作"这个维度而言就等于没有原作 —— 按原作分类时它该进「未分类」,
# 而不是单独占一个 original 目录(库里有 1.2 万条, 会盖过所有真实原作)。
FIELD_BLANK_VALUES = {
    "parody": {"original"},
}


def split_values(raw, field=None):
    """把一个字段的原始值拆成多个独立值。

    分隔符是逗号(兼容全角),去空白、去重、丢弃空项。
    None / 空串 / 纯空白都返回空列表。
    占位符(Unknown)也丢弃 —— 它只表示"未知",不是真实取值。
    field: 传了字段名就额外套用该字段的 FIELD_BLANK_VALUES。
    """
    if raw is None:
        return []
    s = str(raw)
    for sep in _VALUE_SEPS[1:]:
        s = s.replace(sep, _VALUE_SEPS[0])
    blanks = FIELD_BLANK_VALUES.get(field, ()) if field else ()
    out, seen = [], set()
    for part in s.split(_VALUE_SEPS[0]):
        v = part.strip()
        if not v or v in seen or _is_placeholder(v):
            continue
        if v.lower() in blanks:
            continue
        seen.add(v)
        out.append(v)
    return out


# ---------------------------------------------------------------- 元数据解析

def _strip_ns(tag):
    return tag.split("}")[-1]


def parse_comicinfo(text):
    """ComicInfo.xml -> {标签名: 文本}"""
    if not text:
        return {}
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        return {}
    out = {}
    for child in root:
        key = _strip_ns(child.tag)
        val = (child.text or "").strip()
        if val:
            out[key] = val
    return out


def _load_json(text):
    if not text:
        return {}
    try:
        data = json.loads(text)
    except (ValueError, TypeError):
        return {}
    return data if isinstance(data, dict) else {}


def _norm_language(v):
    """把 'zh' / ['translated','chinese'] / 'chinese' 统一成 'chinese'。

    'translated' 是汉化标记不是语言,跳过;占位符(Unknown)视为空。
    """
    if isinstance(v, (list, tuple)):
        for item in v:
            s = str(item).strip().lower()
            if s and s != "translated" and not _is_placeholder(s):
                return LANG_MAP.get(s, s)
        return None
    if not v:
        return None
    s = str(v).strip().lower()
    if _is_placeholder(s):
        return None
    return LANG_MAP.get(s, s)


def _detect_translated(v):
    """判断是否汉化/翻译版本。返回 1 或 None。

    认这些写法:
      ComicInfo <Translated>Yes</Translated> / No
      metadata.json "language": ["translated", "chinese"]
      info.json  "translated": true
    """
    if isinstance(v, (list, tuple)):
        return 1 if any(str(x).strip().lower() == "translated" for x in v) else None
    if v in (None, ""):
        return None
    s = str(v).strip().lower()
    return 1 if s in ("yes", "true", "1", "translated") else None


def _gid_from_link(link):
    if not link:
        return None
    link = str(link)
    if "/g/" in link:
        tail = link.rstrip("/").split("/g/")[-1]
        num = ""
        for ch in tail:
            if ch.isdigit():
                num += ch
            else:
                break
        return num or None
    return None


def _canon(title=None, title_original=None, link=None, category=None, tags=None,
           language=None, translated=None, pages=None, gid=None):
    """归一化成统一的 gallery 字典。"""
    tags = tags if isinstance(tags, dict) else {}
    return {
        "title": _join(title),
        "title_original": _join(title_original),
        "link": _join(link),
        "category": _join(category) or _join(tags.get("category")),
        "tags": tags,
        "language": _norm_language(_first(language, tags.get("language"))),
        "translated": translated,
        "pages": _int(pages),
        "gid": str(gid) if gid else _gid_from_link(link),
    }


def parse_infojson(text):
    """info.json (nhentai-helper):gallery_info 结构。"""
    data = _load_json(text)
    gi = data.get("gallery_info") if isinstance(data.get("gallery_info"), dict) else data
    if not gi:
        return None
    src = gi.get("source") if isinstance(gi.get("source"), dict) else {}
    up = gi.get("upload_date") if isinstance(gi.get("upload_date"), list) else []
    g = _canon(
        title=gi.get("title"),
        title_original=gi.get("title_title_original"),
        link=gi.get("link"),
        category=gi.get("category"),
        tags=gi.get("tags"),
        language=gi.get("language"),
        translated=1 if gi.get("translated") else None,
        gid=src.get("gid"),
    )
    if len(up) >= 3:
        g["upload_date"] = [_int(up[0]), _int(up[1]), _int(up[2])]
    return g


def parse_metajson(text):
    """meta.json (comic-looms):originTitle/url。"""
    gi = _load_json(text)
    if not gi:
        return None
    return _canon(
        title=gi.get("title"),
        title_original=gi.get("originTitle"),
        link=gi.get("url"),
        tags=gi.get("tags"),
    )


def parse_metadatajson(text):
    """metadata.json:标签列表直接在顶层。"""
    gi = _load_json(text)
    if not gi:
        return None
    lang = gi.get("language")
    return _canon(
        title=gi.get("title"),
        title_original=gi.get("subtitle"),
        link=gi.get("URL") or gi.get("url"),
        category=gi.get("category"),
        tags=gi,
        language=lang,
        # 汉化标记有两种写法: language 数组含 "translated", 或独立的 translated 字段
        translated=_detect_translated(lang) or _detect_translated(gi.get("translated")),
        pages=gi.get("Pages"),
    )


GALLERY_PARSERS = {
    "info_json": parse_infojson,
    "meta_json": parse_metajson,
    "metadata_json": parse_metadatajson,
}


def merge_meta(ci, meta_texts, image_count):
    """归一化。meta_texts: {列名: 原文}。

    解析顺序由 GALLERY_PRIORITY 决定(不依赖调用方传参顺序),
    ComicInfo 里未归一化的字段收集进 other_fields。
    """
    galleries = []
    for col in GALLERY_PRIORITY:
        text = meta_texts.get(col)
        parser = GALLERY_PARSERS.get(col)
        if text and parser:
            g = parser(text)
            if g:
                galleries.append(g)

    def pick(field):
        for g in galleries:
            v = g.get(field)
            if v not in (None, "", [], {}):
                return v
        return None

    def pick_tag(key):
        for g in galleries:
            v = _join(g["tags"].get(key))
            if v:
                return v
        return None

    title = pick("title") or _join(ci.get("Title"))
    up = None
    for g in galleries:
        if g.get("upload_date"):
            up = g["upload_date"]
            break

    other = {k: v for k, v in ci.items() if k not in CI_MAPPED}

    return {
        "title": title,
        "title_original": pick("title_original") or title,
        "artist": pick_tag("artist") or _join(ci.get("Writer")),
        "group_name": pick_tag("group") or _join(ci.get("Publisher")),
        "parody": pick_tag("parody") or _join(ci.get("Series")),
        "character": pick_tag("character"),
        "tags": pick_tag("tag") or _join(ci.get("Tags")),
        "category": pick("category") or _join(ci.get("Format")),
        "language": pick("language") or _norm_language(ci.get("LanguageISO")),
        "page_count": _int(ci.get("PageCount")) or pick("pages") or image_count,
        "source_url": pick("link") or _join(ci.get("Web")),
        "source_gid": pick("gid"),
        "year": _int(ci.get("Year")) or (up[0] if up else None),
        "month": _int(ci.get("Month")) or (up[1] if up else None),
        "day": _int(ci.get("Day")) or (up[2] if up else None),
        "translated": 1 if (pick("translated")
                            or _detect_translated(ci.get("Translated"))) else 0,
        "format": _join(ci.get("Format")),
        "other_fields": json.dumps(other, ensure_ascii=False) if other else None,
    }


def classify_entries(names):
    """成员名 -> ({列名: 成员名}, 其它元数据名, 图片数)"""
    found, extra, images = {}, [], 0
    for n in names:
        base = os.path.basename(n).lower()
        if base in META_FILE_COLUMNS:
            found.setdefault(META_FILE_COLUMNS[base], n)
        elif base.endswith(OTHER_META_EXT):
            extra.append(n)
        elif base.endswith(IMAGE_EXT):
            images += 1
    return found, extra, images


# ---------------------------------------------------------------- 扫描单个压缩包

def scan_one(path, size=None, mtime=None, fileobj=None):
    """扫一个压缩包,返回记录 dict。异常由调用方捕获。

    path     只用来取文件名(远程时是一个 `123:/a/b.zip` 形式的字符串)。
    size/mtime 给定时直接用 —— 远程文件不落地, 大小和 mtime 只能来自
              WebDAV 的目录清单; 不给则按本地文件读(老调用行为不变)。
    fileobj  可 seek 的只读文件对象。远程用 RemoteFile, 本地不传。
    """
    if size is None:
        size = os.path.getsize(path)
    if not size:
        raise RuntimeError("文件大小为 0")
    if fileobj is None:
        zf_ctx = zipfile.ZipFile(path)
    else:
        zf_ctx = zipfile.ZipFile(fileobj)
    with zf_ctx as zf:
        names = zf.namelist()
        found, extra_names, images = classify_entries(names)
        texts = {}
        for col, member in found.items():
            texts[col] = _decode(zf.read(member))
        extra = {}
        for n in extra_names:
            try:
                extra[n] = _decode(zf.read(n))
            except Exception as e:
                extra[n] = f"<<读取失败: {e}>>"

    ci = parse_comicinfo(texts.get("comicinfo_xml"))
    meta = merge_meta(ci, texts, images)
    has_meta = any(meta.get(k) for k in
                   ("title", "artist", "tags", "language", "parody"))

    if mtime is None:
        mtime = int(os.path.getmtime(path))

    rec = {
        "name": os.path.basename(path),
        "size": size,
        "mtime": mtime,
        "entry_count": len(names),
        "image_count": images,
        "extra_json": json.dumps(extra, ensure_ascii=False) if extra else None,
        "scanned_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "error": None,
        "has_meta": 1 if has_meta else 0,
        "meta": meta,
    }
    for col in META_FILE_COLUMNS.values():
        rec[col] = texts.get(col)
    return rec


# ---------------------------------------------------------------- 数据库

SCHEMA = """
CREATE TABLE IF NOT EXISTS archives (
    id            INTEGER PRIMARY KEY,
    name          TEXT NOT NULL UNIQUE,   -- 文件名,增量判断的 key
    size          INTEGER,
    mtime         INTEGER,
    entry_count   INTEGER,
    image_count   INTEGER,
    comicinfo_xml TEXT,                   -- 原文
    info_json     TEXT,
    meta_json     TEXT,
    metadata_json TEXT,
    extra_json    TEXT,                   -- 其它元数据文件 {文件名: 原文}
    has_meta      INTEGER DEFAULT 0,      -- 1=解析出了元数据;0=zip 里没有元数据文件
    scanned_at    TEXT,
    error         TEXT
);

CREATE TABLE IF NOT EXISTS meta (
    archive_id     INTEGER PRIMARY KEY REFERENCES archives(id) ON DELETE CASCADE,
    title          TEXT,
    title_original TEXT,
    artist         TEXT,
    group_name     TEXT,
    parody         TEXT,
    character      TEXT,
    tags           TEXT,
    category       TEXT,
    language       TEXT,
    page_count     INTEGER,
    source_url     TEXT,
    source_gid     TEXT,
    year           INTEGER,
    month          INTEGER,
    day            INTEGER,
    translated     INTEGER,
    format         TEXT,
    other_fields   TEXT                   -- ComicInfo 其余字段 JSON
);

CREATE INDEX IF NOT EXISTS idx_meta_artist ON meta(artist);
CREATE INDEX IF NOT EXISTS idx_meta_parody ON meta(parody);
CREATE INDEX IF NOT EXISTS idx_meta_language ON meta(language);
CREATE TABLE IF NOT EXISTS dir_listing (
    root        TEXT PRIMARY KEY,
    files_json  TEXT NOT NULL,
    updated_at  REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_archives_has_meta ON archives(has_meta);

CREATE TABLE IF NOT EXISTS meta_values (
    archive_id INTEGER NOT NULL REFERENCES archives(id) ON DELETE CASCADE,
    field      TEXT NOT NULL,
    value      TEXT NOT NULL,
    PRIMARY KEY (archive_id, field, value)
);
CREATE INDEX IF NOT EXISTS idx_mv_field_value ON meta_values(field, value);
CREATE INDEX IF NOT EXISTS idx_mv_archive ON meta_values(archive_id);
"""

ARCHIVE_COLUMNS = ["name", "size", "mtime", "entry_count", "image_count",
                   "comicinfo_xml", "info_json", "meta_json", "metadata_json",
                   "extra_json", "has_meta", "scanned_at", "error"]

META_COLUMNS = ["title", "title_original", "artist", "group_name", "parody",
                "character", "tags", "category", "language", "page_count",
                "source_url", "source_gid", "year", "month", "day",
                "translated", "format", "other_fields"]

# 需要拆分成多个值的字段(全部字段都拆,统一处理逻辑)
MULTI_FIELDS = list(META_COLUMNS)

# 值分隔符: 逗号,全角半角都认
_VALUE_SEPS = (",", "，")


def open_db(path=DEFAULT_DB):
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA)
    migrate_drop_path(conn)
    migrate_meta_values(conn)
    conn.commit()
    return conn


def migrate_drop_path(conn):
    """老库迁移: 删掉 archives.path 列。

    路径不入库 —— 它随时会变(文件挪窝、外接盘换挂载点), 记在库里只会
    过期。老库里的 path 大都已经失效, 直接丢弃; 下次扫描会重新得到位置。

    必须真的删掉, 不能"删不掉就算了": 老库里 path 是 NOT NULL, 而写入
    早已不再提供这一列, 留着它每一次入库都会报 NOT NULL 约束失败,
    整个扫描中断。SQLite < 3.35 没有 DROP COLUMN, 就走重建表。

    返回 True 表示执行了迁移。
    """
    cols = [c[1] for c in conn.execute("PRAGMA table_info(archives)")]
    if "path" not in cols:
        return False
    try:
        conn.execute("ALTER TABLE archives DROP COLUMN path")
        return True
    except sqlite3.OperationalError:
        pass                    # 老版本 / 有索引引用 -> 走重建表

    # 重建表: 建新的(无 path), 拷数据, 换名。
    # archives 被 meta / meta_values 用外键引用, 期间要关掉外键检查,
    # 否则换名会连带改动子表里的引用。
    old_cols = ",".join(c for c in cols if c != "path")
    # meta / meta_values 用外键指向 archives, 换表期间必须关掉外键检查,
    # 否则 DROP TABLE 会把子表数据一起级联删掉。
    conn.commit()
    conn.execute("PRAGMA foreign_keys=OFF")
    try:
        conn.executescript(f"""
            CREATE TABLE archives_new (
                id            INTEGER PRIMARY KEY,
                name          TEXT NOT NULL UNIQUE,
                size          INTEGER,
                mtime         INTEGER,
                entry_count   INTEGER,
                image_count   INTEGER,
                comicinfo_xml TEXT,
                info_json     TEXT,
                meta_json     TEXT,
                metadata_json TEXT,
                extra_json    TEXT,
                has_meta      INTEGER DEFAULT 0,
                scanned_at    TEXT,
                error         TEXT
            );
            INSERT INTO archives_new ({old_cols})
                SELECT {old_cols} FROM archives;
            DROP TABLE archives;
            ALTER TABLE archives_new RENAME TO archives;
            CREATE INDEX IF NOT EXISTS idx_archives_has_meta
                ON archives(has_meta);
        """)
        conn.commit()
        return True
    finally:
        conn.execute("PRAGMA foreign_keys=ON")


def _upsert(conn, table, key_col, rec):
    cols = list(rec)
    sql = (f"INSERT INTO {table} ({','.join(cols)}) "
           f"VALUES ({','.join('?' * len(cols))}) "
           f"ON CONFLICT({key_col}) DO UPDATE SET "
           + ",".join(f"{c}=excluded.{c}" for c in cols if c != key_col))
    conn.execute(sql, [rec[c] for c in cols])


def save(conn, rec):
    row = {c: rec.get(c) for c in ARCHIVE_COLUMNS}
    _upsert(conn, "archives", "name", row)
    aid = conn.execute("SELECT id FROM archives WHERE name=?",
                       (rec["name"],)).fetchone()[0]
    m = rec.get("meta")
    if m:
        _upsert(conn, "meta", "archive_id", dict(m, archive_id=aid))
    else:
        conn.execute("DELETE FROM meta WHERE archive_id=?", (aid,))
    _save_meta_values(conn, aid, m)
    return aid


def _save_meta_values(conn, archive_id, meta_row):
    """把一条记录的 meta 字段拆开写进关联表。"""
    conn.execute("DELETE FROM meta_values WHERE archive_id=?", (archive_id,))
    if not meta_row:
        return
    rows = []
    for field in MULTI_FIELDS:
        for v in split_values(meta_row.get(field), field):
            rows.append((archive_id, field, v))
    if rows:
        conn.executemany(
            "INSERT OR IGNORE INTO meta_values (archive_id, field, value) "
            "VALUES (?,?,?)", rows)


def migrate_meta_values(conn):
    """老库回填: meta 表已有数据但 meta_values 是空的。返回处理行数。"""
    have = conn.execute("SELECT COUNT(*) FROM meta_values").fetchone()[0]
    if have:
        return 0
    total = 0
    rows = conn.execute(
        "SELECT archive_id, " + ",".join(MULTI_FIELDS) + " FROM meta"
    ).fetchall()
    for row in rows:
        aid = row[0]
        meta_row = dict(zip(MULTI_FIELDS, row[1:]))
        _save_meta_values(conn, aid, meta_row)
        total += 1
    conn.commit()
    return total


def load_existing(conn):
    """{name: (mtime, has_meta, error)}

    只用来判断"上次有没有成功", 与文件在哪、size 无关 —— 路径和大小都不入库作为主键。
    has_meta 只用于统计"多少文件没元数据", 不参与是否重读的判断。
    """
    return {n: (m, h, e) for n, m, h, e in conn.execute(
        "SELECT name, mtime, has_meta, error FROM archives")}


# ---------------------------------------------------------------- 并行策略

# 外接存储(FUSE 代理)每次 open 有固定开销, 实测约 150ms/个, 与文件大小无关
# (7200 转盘本身随机 I/O 只需 12-13ms, 其余是 FUSE+MediaProvider 的固定成本)。
# 瓶颈是等待而非计算, 多线程把等待重叠起来。
#
# 32 = 机械盘 NCQ 队列深度: 并发到 32 就把盘喂饱了, 再多只是排队。
# 实测 4/8/16/32/64/128/512 线程, 32 是拐点, 之后无收益(纯噪声)。
REMOVABLE_WORKERS = 32
BUILTIN_WORKERS = 1

# 目录遍历的并行度。遍历是外接盘上最大的瓶颈(单次 listdir 70ms~数秒),
# 实测 1/4/16/32 线程的遍历速率: 0.1 / 2.3 / 12.2 / 3.5 目录每秒,
# 16 是拐点, 32 反而因互相拖慢而下降。
WALK_WORKERS = 16

# 内置存储挂载点前缀, 归一化后以此开头即视为内置
_EMULATED_PREFIXES = ("/storage/emulated/", "/storage/self/")


def is_removable_path(path):
    """路径是否在可移动存储(外接盘)上。

    先 realpath 归一化, /sdcard 这类软链会解析到 /storage/emulated/0。
    /storage/emulated/... 是内置; /storage/<卷名>/... 则是外接(SD 卡/OTG)。
    """
    real = os.path.realpath(path)
    if real.startswith(_EMULATED_PREFIXES):
        return False
    return real.startswith("/storage/")


def worker_count(dirs):
    """按扫描目录决定并行度: 只要含一个外接盘就整体并行。

    dirs 可以是单个路径字符串或路径列表。
    """
    if isinstance(dirs, str):
        dirs = [dirs]
    return (REMOVABLE_WORKERS if any(is_removable_path(d) for d in dirs)
            else BUILTIN_WORKERS)


def walk_workers(dirs):
    """遍历目录用的并行度。

    外接盘上 listdir 单次 70ms~数秒, 遍历是最大瓶颈, 用 WALK_WORKERS。
    但并发过高会互相拖慢(实测 32 线程反而不如 16), 故单独取值。
    内置存储 listdir 极快, 不需要。
    """
    if isinstance(dirs, str):
        dirs = [dirs]
    return (WALK_WORKERS if any(is_removable_path(d) for d in dirs) else 1)


def _scan_one_dir(d):
    """读一个目录, 分成 (子目录, 可解析压缩包, 不解析的 rar/7z)。

    与 os.walk 语义一致: 每个条目都判定是不是目录。
    实测 scandir+entry.is_dir() 比 listdir+isdir 还慢 1.3 倍,
    所以不用 scandir。
    """
    try:
        entries = os.listdir(d)
    except OSError:
        return [], [], []
    subs, zips, unsup = [], [], []
    for e in entries:
        p = os.path.join(d, e)
        try:
            is_dir = os.path.isdir(p)
        except OSError:
            is_dir = False
        if is_dir:
            subs.append(p)
            continue
        ext = os.path.splitext(e)[1].lower()
        if ext in SUPPORTED:
            zips.append(p)
        elif ext in NOTICE_EXT:
            unsup.append(p)
    return subs, zips, unsup


def _collect_targets(dirs, workers, stop_flag, recursive=True):
    """遍历目录树, 收集可解析压缩包。返回 (targets, unsupported)。

    外接盘上单次 listdir 要 70ms~数秒(FUSE 固定开销), 遍历是整个扫描
    最大的瓶颈——比解析压缩包本身贵得多, 且原本必须全部走完才能开始
    解析。这里按层并行, 实测 16 线程比单线程快两个数量级。

    recursive: False 时只看给定目录这一层, 不下潜子目录。
    stop_flag 在遍历阶段也生效(原本 os.walk 阶段点停止没反应)。
    """
    targets, unsupported = [], []
    roots = [d for d in dirs if os.path.isdir(d)]
    if not roots:
        return targets, unsupported

    if not recursive:
        # 只要一层: 并行逐目录跑。外接盘(FUSE)单次 listdir 可能几秒,
        # 所以要逐批处理并检查 stop_flag —— 一次性 list(map(...)) 的话
        # 点停止要等全部目录跑完才响应, 看着像按钮失灵。
        if workers <= 1:
            for d in roots:
                if stop_flag is not None and stop_flag():
                    return targets, unsupported
                subs, zips, unsup = _scan_one_dir(d)
                targets.extend(zips)
                unsupported.extend(unsup)
            return targets, unsupported
        with ThreadPoolExecutor(max_workers=workers) as ex:
            CHUNK = workers          # 一批约等于一轮并发, 粒度够细
            for i in range(0, len(roots), CHUNK):
                if stop_flag is not None and stop_flag():
                    return targets, unsupported
                for subs, zips, unsup in ex.map(_scan_one_dir,
                                                roots[i:i + CHUNK]):
                    targets.extend(zips)
                    unsupported.extend(unsup)
        return targets, unsupported

    def harvest(results):
        """把一批 (子目录,压缩包,不支持) 汇总进结果并返回下一层目录。

        只由主线程调用, 所以 targets/unsupported 没有并发写问题。
        """
        nxt = []
        for subs, zips, unsup in results:
            nxt.extend(subs)
            targets.extend(zips)
            unsupported.extend(unsup)
        return nxt

    if workers <= 1:
        for root in roots:
            frontier = [root]
            while frontier:
                if stop_flag is not None and stop_flag():
                    return targets, unsupported
                frontier = harvest([_scan_one_dir(d) for d in frontier])
    else:
        frontier = roots
        with ThreadPoolExecutor(max_workers=workers) as ex:
            while frontier:
                if stop_flag is not None and stop_flag():
                    return targets, unsupported
                frontier = harvest(list(ex.map(_scan_one_dir, frontier)))
    return targets, unsupported


def _join_rel(directory, name):
    """把 PROPFIND 按目录分组后的文件名拼回完整相对路径。"""
    directory = (directory or "").strip("/")
    name = (name or "").lstrip("/")
    return f"{directory}/{name}" if directory else name


def prune_dir_listing(db_path, root, seen_child_roots, recursive=None):
    """删除当前层已消失的文件夹缓存(整棵子树)。

    递归与否都是同一条规则:
    - 扫描某目录时, PROPFIND/os.scandir 能看到它的直接子文件夹
    - 缓存里属于该目录但本次没看到的直接子文件夹 => 已消失
    - 已消失文件夹本身 + 它下面所有子孙目录缓存一起删除
    - 仍存在的文件夹: 不碰(没递归进去就不能判断内部变化)
    """
    prefix = root.rstrip("/") + "/"
    seen_top = set()
    for sr in seen_child_roots or []:
        if not sr or not sr.startswith(prefix):
            continue
        seen_top.add(sr[len(prefix):].split("/", 1)[0])

    c = sqlite3.connect(db_path)
    deleted = 0
    try:
        c.execute("""CREATE TABLE IF NOT EXISTS dir_listing (
            root TEXT PRIMARY KEY,
            files_json TEXT NOT NULL,
            updated_at REAL NOT NULL
        )""")
        rows = c.execute("SELECT root FROM dir_listing").fetchall()
        for (r,) in rows:
            if not r or not r.startswith(prefix) or r == root.rstrip("/"):
                continue
            rest = r[len(prefix):]
            top = rest.split("/", 1)[0]
            if top not in seen_top:
                c.execute("DELETE FROM dir_listing WHERE root=?", (r,))
                deleted += 1
        c.commit()
    finally:
        c.close()
    return deleted


# ---------------------------------------------------------------- 主扫描流程

def scan_dirs(dirs, db_path=DEFAULT_DB, log=print, rescan=False,
              progress=None, stop_flag=None, workers=None, recursive=True):
    """扫描目录列表,增量入库。返回统计 dict。

    log:        每行日志的回调(GUI 里接 UI)
    progress:   (done, total, current, phase) 回调。current 是正在处理的
                文件名, phase 是当前阶段文字。远程扫描每个文件要一两秒,
                界面靠它显示"已处理 N/M"的一行进度, 而不是刷一屏日志。
    stop_flag:  可变对象,真值即中止(GUI 停止按钮)
    recursive:  False 时只扫给定目录这一层, 不下潜子目录。
                默认 True —— CLI 与老调用行为不变; 界面按需传 False。
    """
    def emit(msg):
        log(msg)
        try:
            logging.getLogger("scan").info(str(msg))
        except Exception:
            pass

    emit("扫描开始: dirs=" + repr(dirs) + " db=" + str(db_path) +
         " recursive=" + str(bool(recursive)) + " rescan=" + str(bool(rescan)))
    # ---- 按数据源分派 -------------------------------------------------
    # 远程和本地都当成"数据源"看, 但**不能混在同一批里扫**:
    # archives.name 是唯一键, 两边出现同名文件时会互相覆盖元数据,
    # 而本次记录的位置(found_paths)只会指向其中一个存储 ——
    # 正是之前修过的"元数据与实搬文件不是同一份"。
    remote_roots = [d for d in dirs if _source.is_remote(d)]
    local_roots = [d for d in dirs if not _source.is_remote(d)]
    if remote_roots and local_roots:
        emit("扫描失败: 本地目录与远程路径不能混在同一次扫描里")
        raise ValueError("本地目录与远程路径不能混在同一次扫描里, 请分两次扫")
    for root in local_roots:
        if not os.path.isdir(root):
            emit(f"⚠ 目录不存在: {root}")

    t_walk = time.time()
    unsupported = []
    # 统一成 {path, name, size, mtime, src, rel}: 本地 size/mtime 为
    # None 表示"当场读", 远程则由目录清单带来(远程文件不落地)。
    items = []
    if remote_roots:
        for root in remote_roots:
            emit(f"开始读取远程路径: {root} (递归={bool(recursive)})")
            if progress:
                progress(0, 0, "", "拉取远程文件清单")
            _source.drop_source(root)
            src = _source.get_source(root)
            walker = getattr(src, "list_dirs", None)
            if not callable(walker):
                emit(f"⚠ {root}: 数据源不支持逐目录列举")
                continue
            try:
                for drel, files, subdirs in walker(recursive=recursive):
                    dir_root = _source.join(root, drel).rstrip("/")
                    # 当前层实际看到的直接子文件夹: 只用来删除消失的缓存,
                    # 存在的文件夹不写、不更新、不标记。
                    child_roots = [_source.join(dir_root, sd).rstrip("/")
                                   for sd in (subdirs or [])]
                    emit(f"  {dir_root}: 检查目录清单缓存, 当前层子目录 {len(child_roots)} 个")
                    removed = prune_dir_listing(db_path, dir_root, child_roots)
                    emit(f"  {dir_root}: 清理失效子目录缓存 {removed} 条")
                    archives_here = []
                    for rel, size, mtime in files:
                        full_rel = _join_rel(drel, rel)
                        if not full_rel.lower().endswith(ARCHIVE_EXT):
                            if full_rel.lower().endswith(NOTICE_EXT):
                                unsupported.append(full_rel)
                            continue
                        name = os.path.basename(full_rel)
                        entry = {"path": _source.join(root, full_rel), "name": name,
                                 "size": size, "mtime": mtime,
                                 "src": src, "rel": full_rel}
                        items.append(entry)
                        archives_here.append(entry)
                    # 只有实际读取过这个目录的文件, 才覆盖它的清单缓存。
                    # 非递归时仅识别到存在的子目录, 不能写空清单把旧数据抹掉。
                    actually_read = bool(files) or recursive or drel == ""
                    if actually_read:
                        listing = [{"name": e["name"], "size": e["size"],
                                    "mtime": e["mtime"]} for e in archives_here]
                        emit(f"  {dir_root}: 写入目录清单缓存 {len(listing)} 个")
                        try:
                            conn_tmp = open_db(db_path)
                            conn_tmp.execute("""INSERT INTO dir_listing (root,files_json,updated_at)
                                VALUES (?,?,?) ON CONFLICT(root) DO UPDATE SET
                                files_json=excluded.files_json, updated_at=excluded.updated_at""",
                                (dir_root, json.dumps(listing, ensure_ascii=False), time.time()))
                            conn_tmp.commit(); conn_tmp.close()
                            emit(f"  {dir_root}: 目录清单缓存写入完成 {len(listing)} 个")
                        except Exception as e:
                            emit(f"  {dir_root}: 目录清单缓存写入失败 {type(e).__name__}: {e}")
                    else:
                        emit(f"  {dir_root}: 跳过目录清单写入(非递归仅识别存在, 子目录 {len(subdirs or [])} 个)")
            except _source.SourceError as e:
                emit(f"⚠ {root}: 读取失败 — {e}")
                continue
        w_walk = _source.REMOTE_WORKERS
        emit(f"远程解析: {w_walk} 线程")
    else:
        w_walk = walk_workers(dirs)
        if w_walk > 1:
            emit(f"遍历目录: {w_walk} 线程")
        targets, unsupported = _collect_targets(dirs, w_walk, stop_flag,
                                                recursive)
        for path in targets:
            items.append({"path": path, "name": os.path.basename(path),
                          "size": None, "mtime": None, "src": None,
                          "rel": None})
        # 本地也按同一规则: 每个实际扫描到的目录, 只根据当前层子文件夹删失效缓存。
        checked_dirs = set()
        for it in items:
            d = os.path.dirname(it["path"]).rstrip(os.sep)
            if not d or d in checked_dirs:
                continue
            checked_dirs.add(d)
            try:
                children = [os.path.abspath(e.path).rstrip(os.sep)
                            for e in os.scandir(d) if e.is_dir()]
                emit(f"  {d}: 检查本地目录清单缓存, 当前层子目录 {len(children)} 个")
                removed = prune_dir_listing(db_path, d, children)
                emit(f"  {d}: 清理失效子目录缓存 {removed} 条")
            except OSError as e:
                emit(f"  {d}: 本地目录检查失败 {type(e).__name__}: {e}")
        for r in local_roots:
            d = os.path.abspath(r).rstrip(os.sep)
            if d in checked_dirs:
                continue
            try:
                children = [os.path.abspath(e.path).rstrip(os.sep)
                            for e in os.scandir(d) if e.is_dir()]
                emit(f"  {d}: 检查本地目录清单缓存(根), 当前层子目录 {len(children)} 个")
                removed = prune_dir_listing(db_path, d, children)
                emit(f"  {d}: 清理失效子目录缓存(根) {removed} 条")
            except OSError as e:
                emit(f"  {d}: 本地根目录检查失败 {type(e).__name__}: {e}")
    emit(f"发现 {len(items)} 个可解析压缩包"
         + (f",另 {len(unsupported)} 个 rar/7z 不解析" if unsupported else "")
         + (" (不含子目录)" if not recursive else "")
         + f" (遍历耗时 {time.time() - t_walk:.1f}s)")
    if progress:
        progress(0, 0, "", "检查增量")

    conn = open_db(db_path)
    # 目录清单缓存: 一次列目录得到完整文件列表, 按路径整体覆盖。
    try:
        # 每个实际目录一条清单: 递归扫描时子目录各有自己的 root,
        # 阅读页才能按前缀查到子文件夹并合并。
        by_root = {}
        for it in items:
            path = it.get("path") or ""
            if _source.is_remote(path):
                root = path.rsplit("/", 1)[0] if "/" in path else path
            else:
                root = os.path.dirname(path)
            if not root:
                continue
            by_root.setdefault(root, []).append(
                {"name": it["name"], "size": it.get("size"),
                 "mtime": it.get("mtime")})
        # 空目录也要留一条, 否则删空后缓存残留; 远程已逐目录写过, 这里不覆盖
        for root in local_roots:
            by_root.setdefault(root, [])
        emit(f"开始写入本地目录清单缓存: {len(by_root)} 个路径")
        for root, listing in by_root.items():
            emit(f"  写入本地目录清单缓存: {root} {len(listing)} 个")
            try:
                conn.execute("""INSERT INTO dir_listing (root,files_json,updated_at)
                    VALUES (?,?,?) ON CONFLICT(root) DO UPDATE SET
                    files_json=excluded.files_json, updated_at=excluded.updated_at""",
                    (root, json.dumps(listing, ensure_ascii=False), time.time()))
                emit(f"  已写入本地目录清单缓存: {root} {len(listing)} 个")
            except Exception as e:
                emit(f"  本地目录清单写入失败: {root} {type(e).__name__}: {e}")
        conn.commit()
        emit(f"本地目录清单缓存写入完成: {len(by_root)} 个路径")
    except Exception as e:
        emit(f"  目录清单写入失败: {type(e).__name__}: {e}")
    emit("开始检查增量: 按文件名判断, rescan=" + str(bool(rescan)))
    try:
        existing = {} if rescan else load_existing(conn)
        todo, skip_n, skipped_nometa = [], 0, 0
        # 本次扫到的位置: {name: 绝对路径}。路径只是"当下在哪"的临时记忆,
        # 供本次搬移使用; 不入库(文件挪个位置库里的路径就过期了)。
        found_paths = {}
        for it in items:
            path, name = it["path"], it["name"]
            # 同名多次出现(同名文件散在多个目录): 取**最后**扫到的那个。
            # 必须与入库口径一致 —— 库里以 name 为唯一键, 后写的记录覆盖
            # 先写的, 所以库里的元数据来自最后一个。这里若取第一个,
            # 预览显示的元数据(A的)和实际搬移的文件(B的)就不是同一份,
            # 而且每次重扫可能换一个。
            found_paths[name] = path
            prev = existing.get(name)
            if prev is not None:
                _prev_mtime, prev_meta, prev_err = prev
                # 只按 name 判断, 不比 size/路径/mtime。
                # 库以 name 为唯一键: 同名文件即使大小不同, 元数据也只保留
                # 最后处理的那个。拿 size 当判据会导致同名不同大小反复解析、
                # 反复覆盖; 内容真要更新用「强制重扫」。
                if prev_err is None:
                    skip_n += 1
                    if not prev_meta:
                        skipped_nometa += 1
                    continue
            todo.append(it)
        emit(f"待处理 {len(todo)},增量跳过 {skip_n}"
             + (f"(其中 {skipped_nometa} 个解析不出元数据, 同名跳过)"
                if skipped_nometa else ""))

        done = fail = nometa = 0
        failures = []
        t0 = time.time()
        if remote_roots:
            n_workers = _source.REMOTE_WORKERS
        else:
            n_workers = workers if workers is not None else worker_count(dirs)
        emit(f"解析线程: {n_workers}")
        if n_workers > 1:
            emit(f"并行扫描: {n_workers} 线程")

        # 并行只做"读+解析"(纯函数,无共享状态); 写库仍在主线程串行
        # (SQLite 连接不能跨线程)。
        def parse(it):
            try:
                if it["src"] is not None:
                    # 远程: 打开远端文件对象, 交给同一套 zip 解析逻辑
                    with it["src"].open_file(it["rel"]) as fh:
                        rec = scan_one(it["path"], size=it["size"],
                                       mtime=it["mtime"], fileobj=fh)
                else:
                    rec = scan_one(it["path"], size=it["size"],
                                   mtime=it["mtime"])
                return it, rec, None
            except Exception as e:
                err = f"{type(e).__name__}: {e}"
                # 远程读到的空内容已经由 source 层重试过, 走到这里就是
                # 服务端限流。别让它看起来像文件损坏 —— 两者处理方式
                # 完全不同(等待重扫 vs 检查文件)。
                if (it["src"] is not None
                        and "返回空内容" in str(e)):
                    err += " (多半是 123 网盘限流, 稍后重扫即可)"
                return it, None, err

        def consume(it, rec, err, i):
            """把一条解析结果写库。返回 True 表示已中止。"""
            nonlocal done, fail, nometa
            name = it["name"]
            # 远程读取失败多是网络/限流, 不代表文件本身坏了。这种情况
            # **不能删掉已有的元数据** —— 删了下次还得重读, 而这期间
            # 浏览/统计里就凭空少了一个作品。只记下"这次没读成",
            # 保留旧记录, 下次重扫再补。
            transient = (it["src"] is not None and err is not None
                         and ("不完整" in err or "返回空" in err
                              or "超时" in err))
            if err is not None and transient:
                fail += 1
                failures.append((name, err))
                prev = conn.execute(
                    "SELECT has_meta FROM archives WHERE name=?",
                    (name,)).fetchone()
                if prev is None:
                    _upsert(conn, "archives", "name", {
                        "name": name,
                        "scanned_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                        "error": err, "has_meta": 0,
                    })
                # 远程逐个文件打日志会刷屏(930 个), 进度行已经在滚,
                # 这里只在远程时静默记录, 结尾统一汇总。
                if it["src"] is None:
                    emit(f"  ✗ {name[:70]}: {err}")
            elif err is not None:
                fail += 1
                failures.append((name, err))
                _upsert(conn, "archives", "name", {
                    "name": name,
                    "scanned_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "error": err,
                    "has_meta": 0,
                })
                # 解析失败的记录不能留着上一次的元数据: 文件损坏/被换成
                # 别的东西后, 旧的 title/artist 还在, 浏览、统计、详情都
                # 照旧显示它是个有效作品, 统计还会把它计入"有元数据"。
                row = conn.execute("SELECT id FROM archives WHERE name=?",
                                   (name,)).fetchone()
                if row:
                    conn.execute("DELETE FROM meta WHERE archive_id=?", (row[0],))
                    conn.execute("DELETE FROM meta_values WHERE archive_id=?",
                                 (row[0],))
                emit(f"  ✗ {name[:70]}: {err}")
            else:
                emit(f"  ✓ {name[:70]}")
                save(conn, rec)
                if not rec["has_meta"]:
                    # 解析成功(能读、没报错)但里面没有元数据文件。
                    # 内容不变就不会再重读 —— 重读结果必然一样。
                    nometa += 1
                    # 远程不逐条打: 930 个里有一大批没元数据, 会把日志
                    # 冲成瀑布。结尾的汇总里已经有这个数。
                    if it["src"] is None:
                        emit(f"  ○ {name[:70]} (解析不出元数据,已入库)")
                done += 1
            if i % 10 == 0 or i == len(todo):
                conn.commit()
            if progress:
                progress(i, len(todo), name, "解析元数据")

        if n_workers <= 1:
            for i, it in enumerate(todo, 1):
                if stop_flag is not None and stop_flag():
                    emit("⏹ 用户中止")
                    break
                consume(*parse(it), i)
        else:
            with ThreadPoolExecutor(max_workers=n_workers) as ex:
                # map 按输入顺序产出结果, 入库顺序与单线程一致。
                # 提前 break 会等已派发的任务收尾后才退出, 最长约
                # (未完成数 / 线程数) 个文件的耗时。
                it = ex.map(parse, todo)
                for i, (item, rec, err) in enumerate(it, 1):
                    if stop_flag is not None and stop_flag():
                        emit("⏹ 用户中止")
                        break
                    consume(item, rec, err, i)
        conn.commit()
        el = time.time() - t0
        emit(f"扫描阶段完成: 成功 {done}"
             + (f"(其中 {nometa} 个解析不出元数据)" if nometa else "")
             + f",失败 {fail},跳过 {skip_n},耗时 {el:.1f}s")
        if failures:
            emit(f"失败 {len(failures)} 个(下次自动重试)")
            # 远程失败不逐条打, 这里给出前几条和原因分布, 否则用户
            # 只看到一个数字, 不知道是限流、文件坏了还是别的。
            if remote_roots:
                from collections import Counter
                why = Counter(f"{n}: {e}" for n, e in failures)
                for line, cnt in why.most_common(5):
                    emit(f"  · {line}" + (f"  (共 {cnt} 个)" if cnt > 1 else ""))
                if len(why) > 5:
                    emit(f"  · …另有 {len(why) - 5} 个失败文件未列出")
        return {"total": len(items), "todo": len(todo), "done": done,
                "fail": fail, "skip": skip_n,
                # 解析成功但里面没有元数据文件的个数(它们也在 done 里)
                "nometa": nometa,
                "unsupported": unsupported, "elapsed": el,
                # 本次扫到的位置, 供调用方当场搬移用(库不存路径)
                "paths": found_paths,
                # 本次扫到的文件名(顺序=扫描顺序, 已去重)。浏览页靠它把
                # 结果收窄到"这次扫到的那些", 不靠 scanned_at ——
                # 增量跳过的记录不会刷新 scanned_at。
                "names": list(found_paths)}
    finally:
        conn.close()


# ---------------------------------------------------------------- 查询(GUI 用)

def scope_clause(conn, names):
    """把"只看这批文件名"变成可直接拼进 WHERE 的条件, 返回 SQL 片段。

    names 为 None  -> 不设范围(全库), 返回 "1=1"
    names 为空列表 -> **空范围**, 返回 "1=0"。这个区别很要紧: 浏览页在
                      "还没扫描过"时该是空的, 绝不能退化成把全库倒出来。
    names 非空     -> 建临时表装名单, 返回 "a.name IN (SELECT name FROM _scope)"

    走临时表而不是 IN (?,?,...): 一次扫描可能有几万个文件名, 拼进 SQL 会
    撞上 SQLite 的参数个数上限, 长 SQL 本身也慢。
    """
    if names is None:
        return "1=1"
    uniq = list(dict.fromkeys(names))        # 去重(保序), 顺带避免重复插入
    if not uniq:
        return "1=0"
    conn.execute("CREATE TEMP TABLE IF NOT EXISTS _scope "
                 "(name TEXT PRIMARY KEY)")
    conn.execute("DELETE FROM _scope")
    conn.executemany("INSERT OR IGNORE INTO _scope (name) VALUES (?)",
                     [(n,) for n in uniq])
    return "a.name IN (SELECT name FROM _scope)"


def list_archives(db_path=DEFAULT_DB, where="1=1", args=(), limit=2000,
                  names=None):
    """archives JOIN meta,返回字段名列表 + 行列表。

    names: 只看这批文件名(None=全库; 空列表=一条都不给)。浏览页用它把
           结果收窄到"本次扫描到的那些"。
    """
    conn = sqlite3.connect(db_path)
    try:
        cols = ["id", "name", "size", "image_count", "has_meta",
                "error"] + META_COLUMNS
        scope = scope_clause(conn, names)
        sql = ("SELECT a." + ",".join(
                   c if c in ("id", "name", "size", "image_count",
                              "has_meta", "error") else f"m.{c}"
                   for c in cols)
               + " FROM archives a LEFT JOIN meta m ON m.archive_id=a.id "
                 f"WHERE ({where}) AND ({scope}) "
                 f"ORDER BY a.name LIMIT {int(limit)}")
        rows = conn.execute(sql, args).fetchall()
        return cols, rows
    finally:
        conn.close()


def filter_archives(db_path=DEFAULT_DB, field=None, value=None,
                    contains=None, limit=2000):
    """按 meta 字段精确匹配或 tags 模糊包含过滤。

    field:   meta 表列名(白名单校验)
    value:   精确相等
    contains: LIKE %..%(tag 场景)
    """
    valid = set(META_COLUMNS) | {"language", "category"}
    if field and field not in valid:
        raise ValueError(f"非法字段: {field}")
    if field and value is not None:
        where, args = f"m.{field} = ?", (value,)
    elif field and contains is not None:
        where, args = f"m.{field} LIKE ?", (f"%{contains}%",)
    else:
        where, args = "m.title IS NOT NULL", ()
    return list_archives(db_path, where, args, limit)


# ---------------------------------------------------------------- 多条件筛选

VALID_FIELDS = set(META_COLUMNS) | {"language", "category"}

# 适合做分布统计的字段(有分类意义、值可复用)。
# 排除掉 title/source_url/source_gid/month/day 这些:
# 它们的每个值几乎只对应一个作品, 统计出来没有意义。
STAT_FIELDS = ["artist", "group_name", "parody", "character",
               "tags", "language", "category", "format"]
VALID_OPS = {
    "eq": "等于", "ne": "不等于", "contains": "包含",
    "not_contains": "不包含", "empty": "为空", "not_empty": "不为空",
    "count_eq": "=", "count_gte": ">=", "count_lte": "<=",
}
VALID_JOINS = ("and", "or")

# 数量类操作符(需要先按字段分组计数)
COUNT_OPS = ("count_eq", "count_gte", "count_lte")


def is_blank_condition(cond):
    """这个条件是不是"没填完"(需要值却给了空)。

    空值条件必须被**所有**筛选路径忽略: 当成 `artist = ''` 会把结果滤成
    0 条, 界面上看着像"没有匹配", 其实只是条件行没填。
    build_where(SQL 路径) 和 _narrow_one(值级收窄路径) 共用这个判断 ——
    只改一处的话, 浏览页正常而分类页仍然 0 条, 更难查。
    """
    op = (cond.get("op") or "eq").strip()
    if op in ("empty", "not_empty"):
        return False                    # 这两个操作符本来就不需要值
    v = cond.get("value")
    if v is None or str(v).strip() == "":
        # 数量条件也要数字: 空着说明这行还没填完, 整条忽略。
        # 以前对 count_* 直接返回 False(当成"填好了"), 于是空的数量条件
        # 会被 _to_int("") 变成 0 —— `作者 >= 0` 等于没条件, 却让界面
        # 看起来像"筛过了", 结果一动不动。
        return True
    return False


def _to_int(v, what="阈值"):
    try:
        return int(str(v).strip())
    except (TypeError, ValueError):
        raise ValueError(f"非法{what}: {v!r}")


def count_groups(db_path, field, min_count=None, max_count=None,
                 base_conditions=None, join="and", names=None):
    """按字段的拆分值分组计数,返回作品数达标的值列表。

    base_conditions: 先按这些条件圈定候选作品(数量条件与它们取交集)
    min_count / max_count: None 表示**不限**。
        用 None 而不是 0 当"不限"的哨兵 —— 0 是一个合法阈值
        (如 `作者 = 0`、`作者 <= 0`), 拿 0 当哨兵会让这两种条件
        退化成"不限", 静默返回全部值(实测返回 4958 个)。
    names: 只看这批文件名(None=全库; 空列表=一条都不给)
    """
    if field not in VALID_FIELDS:
        raise ValueError(f"非法字段: {field}")
    lo = _to_int(min_count, "下限") if min_count is not None else 0
    hi = _to_int(max_count, "上限") if max_count is not None else None
    where, args = build_where(base_conditions or [], join)
    conn = sqlite3.connect(db_path)
    try:
        scope = scope_clause(conn, names)
        sql = ("SELECT mv.value, COUNT(DISTINCT mv.archive_id) c "
               "FROM meta_values mv JOIN archives a ON a.id=mv.archive_id "
               f"WHERE mv.field=? AND ({where}) AND ({scope}) "
               "GROUP BY mv.value")
        params = [field] + list(args)
        if hi is not None:
            sql += " HAVING c >= ? AND c <= ?"
            params += [lo, hi]
        else:
            sql += " HAVING c >= ?"
            params += [lo]
        return [r[0] for r in conn.execute(sql, params)]
    finally:
        conn.close()


def build_where(conditions, join="and"):
    """多条件 -> (where_sql, args)。

    conditions: [{"field": "artist", "op": "eq", "value": "zmix"}, ...]
    join:       "and" | "or"

    匹配基于 meta_values 关联表的拆分值: 多值字段里任一值命中即算命中。
    字段名走白名单校验(防 SQL 注入),值一律走参数绑定。
    空条件列表返回匹配全部的条件。

    返回的 SQL 片段引用 archives 表的别名 a。
    """
    if join not in VALID_JOINS:
        raise ValueError(f"非法组合符: {join}")
    if not conditions:
        return "1=1", []

    parts, args = [], []
    for c in conditions:
        field = (c.get("field") or "").strip()
        op = (c.get("op") or "eq").strip()
        value = c.get("value")
        if field not in VALID_FIELDS:
            raise ValueError(f"非法字段: {field}")
        if op not in VALID_OPS:
            raise ValueError(f"非法操作符: {op}")
        # 没填完的条件整条忽略(与 _narrow_one 共用同一判断)
        if is_blank_condition(c):
            continue
        if op == "empty":
            parts.append(
                "NOT EXISTS (SELECT 1 FROM meta_values mv "
                "WHERE mv.archive_id=a.id AND mv.field=?)")
            args.append(field)
        elif op == "not_empty":
            parts.append(
                "EXISTS (SELECT 1 FROM meta_values mv "
                "WHERE mv.archive_id=a.id AND mv.field=?)")
            args.append(field)
        elif op == "eq":
            parts.append(
                "EXISTS (SELECT 1 FROM meta_values mv "
                "WHERE mv.archive_id=a.id AND mv.field=? AND mv.value=?)")
            args += [field, value]
        elif op == "ne":
            parts.append(
                "NOT EXISTS (SELECT 1 FROM meta_values mv "
                "WHERE mv.archive_id=a.id AND mv.field=? AND mv.value=?)")
            args += [field, value]
        elif op == "contains":
            parts.append(
                "EXISTS (SELECT 1 FROM meta_values mv "
                "WHERE mv.archive_id=a.id AND mv.field=? AND mv.value LIKE ?)")
            args += [field, f"%{value}%"]
        elif op == "not_contains":
            parts.append(
                "NOT EXISTS (SELECT 1 FROM meta_values mv "
                "WHERE mv.archive_id=a.id AND mv.field=? AND mv.value LIKE ?)")
            args += [field, f"%{value}%"]
    if not parts:
        # 全是没填完的条件 -> 等于没条件。不能返回空串: 拼进 SQL 会变成
        # "WHERE () AND (...)", 直接语法错误。
        return "1=1", []
    glue = f" {join.upper()} "
    return glue.join(parts), args


def query_archives(db_path=DEFAULT_DB, conditions=None, join="and",
                   limit=2000, names=None):
    """按多条件查询,返回 (cols, rows)。

    数量类条件(count_gte / count_lte)先分组计数算出达标的值,
    再把这些值当成等值条件并入查询。

    计数范围跟着 names 走 —— 库只是元数据缓存, "这个作者有几个作品"
    该按**当前这批文件**数, 而不是按库里存了多少条历史记录。以前固定
    按全库数(且没有 names 参数), 结果在一个只有 3 个该作者作品的源目录里
    也会把 `作者>=5` 判成通过。

    普通条件照常通过 join 拼接参与,语义是"先各自算集合,再按 join 合并"。
    多个数量条件之间是交集(用户明确说这种情况该自己规避,不做拦截)。
    """
    conditions = list(conditions or [])
    normal = [c for c in conditions if (c.get("op") or "eq") not in COUNT_OPS]
    counts = [c for c in conditions if (c.get("op") or "eq") in COUNT_OPS]
    if not counts:
        where, args = build_where(normal, join)
        return list_archives(db_path, where, args, limit, names=names)

    # 每个数量条件: 在当前范围内分组计数
    extras = []
    for c in counts:
        if is_blank_condition(c):
            continue                    # 没填数字的条件整条忽略
        v = _to_int(c.get("value"))
        op = c.get("op")
        if op == "count_eq":
            lo = hi = v
        elif op == "count_gte":
            lo, hi = v, None
        else:                           # count_lte
            lo, hi = None, v
        vals = count_groups(db_path, c["field"], min_count=lo, max_count=hi,
                            names=names)
        if not vals:
            # 这个数量条件没有任何达标值, 单独看它是空集
            if join == "and":
                return list_archives(db_path, "1=0", [], limit, names=names)
            extras.append((c["field"], []))
            continue
        extras.append((c["field"], vals))

    where, args = build_where(normal, join)
    args = list(args)
    for field, vals in extras:
        if not vals:
            # 空集: AND 下整体为空(上面已提前返回), OR 下这一支不贡献结果
            block = "1=0"
        else:
            ph = ",".join("?" * len(vals))
            block = ("EXISTS (SELECT 1 FROM meta_values mv "
                     "WHERE mv.archive_id=a.id AND mv.field=? "
                     f"AND mv.value IN ({ph}))")
            args += [field] + vals
        where = f"({where}) {join.upper()} ({block})"
    return list_archives(db_path, where, args, limit, names=names)


# ---------------------------------------------------------------- 值级筛选

def _all_values(db_path, field, names=None, limit=20000):
    """某字段**在当前范围内**出现过的全部值(用于否定条件求补集)。

    names: 只看这批文件名(None=全库; 空列表=一条都不给)。
           必须跟着范围走: 库只是元数据缓存, "有哪些值"是相对于
           当前要处理的这批文件而言的。按全库取的话, ne/not_contains
           的补集会掺进范围外的一堆值(实测 4957 个里 4919 个是范围外的)。

    limit 只作异常数据的兜底。**绝不能靠它静默截断**: 补集被截掉的值
    会被误判成"不满足条件"从而漏掉真正的作品。真到了上限就明确报错,
    让人知道结果不完整, 而不是悄悄给个错的。
    """
    conn = sqlite3.connect(db_path)
    try:
        scope = scope_clause(conn, names)
        sql = ("SELECT DISTINCT mv.value FROM meta_values mv "
               "JOIN archives a ON a.id=mv.archive_id "
               f"WHERE mv.field=? AND ({scope})")
        vals = {r[0] for r in conn.execute(sql, (field,))}
        if len(vals) > limit:
            raise ValueError(
                f"字段 {field} 在当前范围内的不同值有 {len(vals)} 个, "
                f"超过 {limit} 上限, '不等于/不包含'无法可靠求补集")
        return vals
    finally:
        conn.close()


def _narrow_one(db_path, cond, names=None):
    """单个条件 -> (字段, 保留的值集合)。

    返回 None 表示这个条件是作品级的(为空/不为空), 不收窄值集合。

    names: 当前范围(None=全库)。**必须一路传下去**: 数量条件要数的是
           "这批文件里"的作品数, 不是库里存了多少条历史记录 ——
           库只是元数据缓存。不传的话会把范围外的作品也数进去,
           实测 `作者>=5` 在一个只有 3 个该作者作品的源目录里也会通过,
           于是那 3 个文件被按 14 个作品的分量误判、各自散进作者目录。
    """
    field = (cond.get("field") or "").strip()
    op = (cond.get("op") or "eq").strip()
    value = cond.get("value")
    if field not in VALID_FIELDS:
        raise ValueError(f"非法字段: {field}")
    if op not in VALID_OPS:
        raise ValueError(f"非法操作符: {op}")
    # 没填完的条件 -> 不收窄(等价于没这个条件)。
    # 不判的话 eq 会走到 {value} = {''}, 而库里没有空值作者 ->
    # 得到空集合 -> 所有作品都被跳过, 分类页报"没有可分类的"。
    if is_blank_condition(cond):
        return None
    # 为空/不为空是作品级的: 一个作品在某字段上要么有值要么没有
    if op in ("empty", "not_empty"):
        return None
    if op in COUNT_OPS:
        if op == "count_eq":
            lo = hi = _to_int(value or 0)
        elif op == "count_gte":
            lo, hi = _to_int(value or 0), None
        else:                                   # count_lte
            lo, hi = None, _to_int(value or 0)
        return field, set(count_groups(db_path, field, min_count=lo,
                                       max_count=hi, names=names))
    allv = _all_values(db_path, field, names=names)
    if op == "eq":
        return field, {value} if value in allv else set()
    if op == "ne":
        return field, allv - {value}
    if op == "contains":
        return field, {v for v in allv if value in v}
    if op == "not_contains":
        return field, {v for v in allv if value not in v}
    return None


def narrow_values(db_path, conditions, join="and", names=None):
    """算出每个条件字段上保留的值集合。

    同一字段的多个条件各自在**原始值集合**上计算, 再按 join 合并 ——
    不是在第一个条件的结果上继续过滤, 所以条件之间没有顺序依赖。

    names: 当前范围(None=全库)。数量条件与否定条件(不等于/不包含)都
           要在这个范围内算, 详见 _narrow_one / _all_values。

    返回 {字段: 保留的值集合}。作品级条件(为空/不为空)不出现在结果里。
    """
    if join not in VALID_JOINS:
        raise ValueError(f"非法组合符: {join}")
    per_field = {}
    for c in conditions or []:
        got = _narrow_one(db_path, c, names=names)
        if got is None:
            continue
        field, vals = got
        per_field.setdefault(field, []).append(vals)
    out = {}
    for field, sets in per_field.items():
        if join == "and":
            acc = sets[0]
            for s in sets[1:]:
                acc = acc & s
        else:
            acc = set()
            for s in sets:
                acc |= s
        out[field] = acc
    return out


def no_value_names(db_path, conditions=None, join="and", field=None,
                   names=None):
    """在范围内、条件命中、但在 field 上**没有任何值**的作品名。

    这些作品按该字段分组时该落到「未分类」。比如 parody 只有 original 的
    作品: original 已被当作空, 于是它没有 parody 值 —— 但它仍然是个作品,
    该进「未分类」, 而不是整条被丢掉(那样它永远不会被分类/移动)。
    """
    conditions = list(conditions or [])
    narrow = narrow_values(db_path, conditions, join, names=names)
    work_level = [c for c in conditions
                  if (c.get("op") or "eq") in ("empty", "not_empty")]
    has_wl = bool(work_level)
    if has_wl:
        wl_where, wl_args = build_where(work_level, join)
    else:
        wl_where, wl_args = "1=1", []

    conn = sqlite3.connect(db_path)
    try:
        scope = scope_clause(conn, names)
        # join=and: 作品级条件可以直接在 SQL 里收窄。
        # join=or : 作品级只是并列的一支, 不能拿它收窄 —— 否则
        # `作者为空 OR 作者=alice` 会退化成"必须作者为空"。
        if has_wl and join == "and":
            sql = ("SELECT a.name, a.id FROM archives a "
                   f"WHERE ({wl_where}) AND ({scope})")
            rows = conn.execute(sql, wl_args).fetchall()
            wl_ids = None
        else:
            rows = conn.execute(
                f"SELECT a.name, a.id FROM archives a WHERE ({scope})"
            ).fetchall()
            wl_ids = ({r[0] for r in conn.execute(
                "SELECT a.id FROM archives a "
                f"WHERE ({wl_where}) AND ({scope})", wl_args)}
                if has_wl else None)
        if not rows:
            return []
        # 这些作品在任意被收窄字段上是否还有值(没有 -> 被条件排除)
        mv = {}
        q = ("SELECT mv.archive_id, mv.field, mv.value FROM meta_values mv "
             "JOIN archives a ON a.id=mv.archive_id "
             f"WHERE {scope}")
        for aid, f, v in conn.execute(q):
            mv.setdefault(aid, {}).setdefault(f, []).append(v)
    finally:
        conn.close()

    out = []
    for name, aid in rows:
        have = mv.get(aid, {})
        wl_hit = True if wl_ids is None else (aid in wl_ids)
        vl_hit = _match_narrow(narrow, have, join)
        if join == "or":
            ok = (wl_hit if has_wl else False) or vl_hit
        else:
            ok = (wl_hit if has_wl else True) and vl_hit
        if not ok:
            continue
        if not have.get(field):
            out.append(name)
    return out


def query_entries(db_path=DEFAULT_DB, conditions=None, join="and",
                  fields=None, limit=5000, names=None):
    """按条件筛选, 返回**条目级**结果。

    条目 = (作品, 字段, 值)。多值字段一个作品会展开成多个条目,
    每个条目独立参与筛选 —— 所以 `角色 不包含 alice` 只会干掉 alice
    这个条目, 同作品的 bob/carol 照常保留。

    fields: 限定返回哪些字段的条目; None 表示全部 MULTI_FIELDS。
    names:  只看这批文件名(None=全库; 空列表=一条都不给)。**必须在 SQL
            层收窄**, 不能取完再筛: limit 是按 name 排序取前 N 条, 而
            目标文件的名字可能排在几万名开外, 那样会被截断掉, 界面就
            显示"没有可分类的"(曾因此在 4 万条库里只能分类到 0 个)。
    返回 [{archive_id, name, field, value, meta}], 按 name/field/value 排序。
    不含路径 —— 库不记路径, 需要路径请用本次扫描结果或源目录扫描。
    """
    conditions = list(conditions or [])
    narrow = narrow_values(db_path, conditions, join, names=names)

    # 作品级条件(为空/不为空)
    work_level = [c for c in conditions
                  if (c.get("op") or "eq") in ("empty", "not_empty")]
    has_wl = bool(work_level)
    if has_wl:
        wl_where, wl_args = build_where(work_level, join)
    else:
        wl_where, wl_args = "1=1", []

    conn = sqlite3.connect(db_path)
    try:
        scope = scope_clause(conn, names)
        cols = ["id", "name"] + META_COLUMNS
        base = ("SELECT a.id, a.name, "
                + ",".join(f"m.{c}" for c in META_COLUMNS)
                + " FROM archives a LEFT JOIN meta m ON m.archive_id=a.id ")
        wl_ids = None
        # join=and: 作品级条件可以用 SQL 直接收窄(所有条件都要满足)。
        # join=or : 作品级只是"其中一支", 不能拿它收窄 —— 否则
        # `作者为空 OR 作者=alice` 会退化成"必须作者为空", alice 那支没了。
        # 这时先算出命中作品级的那批 id, 再和值级结果在 Python 里取并集。
        if has_wl and join == "and":
            sql = base + f"WHERE ({wl_where}) AND ({scope}) ORDER BY a.name"
            rows = conn.execute(sql, wl_args).fetchall()
        elif has_wl:
            sql = base + f"WHERE ({scope}) ORDER BY a.name"
            rows = conn.execute(sql).fetchall()
            wl_ids = {r[0] for r in conn.execute(
                f"SELECT a.id FROM archives a "
                f"WHERE ({wl_where}) AND ({scope})", wl_args)}
        else:
            rows = conn.execute(base + f"WHERE ({scope}) ORDER BY a.name"
                                ).fetchall()
        # 收窄时把 meta_values 也一起收窄: 否则每次都要把 100 多万行
        # 全读一遍, 而真正用到的只是范围内那几十个作品的条目。
        mv_sql = "SELECT mv.archive_id, mv.field, mv.value FROM meta_values mv"
        if names is not None:
            mv_sql += (" JOIN archives a ON a.id = mv.archive_id"
                       f" WHERE {scope}")
        mv_rows = conn.execute(mv_sql).fetchall()
    finally:
        conn.close()

    want = set(fields) if fields else None
    vals_by = {}
    for aid, f, v in mv_rows:
        if want is not None and f not in want:
            continue
        vals_by.setdefault((aid, f), []).append(v)
    # 每个作品在**全部**被收窄字段上的值(用于判断作品是否还该出现)。
    # 不能只看 fields 里的字段: 条件是 `作者>=5 且 角色不含sensei` 而
    # fields=['character'] 时, 作者收窄也必须过滤作品, 否则会返回全部作品。
    all_by = {}
    for aid, f, v in mv_rows:
        if f in narrow:
            all_by.setdefault(aid, {}).setdefault(f, []).append(v)

    out = []
    works = 0
    for r in rows:
        d = dict(zip(cols, r))
        aid = d["id"]
        # 作品级那支是否命中。None 表示"没有作品级条件"或"and 下已在 SQL
        # 里收窄(取到的行必然满足)"。
        wl_hit = True if wl_ids is None else (aid in wl_ids)
        # 值级那支: join 决定多个被收窄字段之间怎么合。
        # 以前不管 join 都按 and 判, 于是 `作者=X OR 语言=日语` 里
        # 只满足其中一个的作品会被整条丢掉 —— or 退化成了 and。
        vl_hit = _match_narrow(narrow, all_by.get(aid, {}), join)
        if join == "or":
            # 作品级与值级是并列的两支, 任一命中即可
            ok = (wl_hit if has_wl else False) or vl_hit
        else:
            ok = (wl_hit if has_wl else True) and vl_hit
        if not ok:
            continue
        # limit 按**作品**计, 不按条目: 一个作品有多个值时会展开成多条,
        # 按条目截断会把它从中间切断, 分类时就只搬走一部分值。
        if works >= limit:
            break
        works += 1
        meta = {k: d.get(k) for k in META_COLUMNS}
        for f in (fields if fields else MULTI_FIELDS):
            have = vals_by.get((aid, f), [])
            # 被条件收窄过的字段用收窄后的集合
            if f in narrow:
                allowed = narrow[f]
                vals = [v for v in have if v in allowed]
                # or 语义下: 这个作品是靠**别的字段**那支进来的, 本字段的
                # 条件并没命中 —— 那就把本字段的值原样保留。不这么做的话
                # `作者=X OR 语言=日语` 里靠语言进来的作品, 查作者条目时
                # 会被本字段的收窄清空, 整条作品从结果里消失。
                if not vals and join == "or":
                    vals = list(have)
            else:
                vals = have
            for v in vals:
                out.append({"archive_id": aid, "name": d["name"],
                            "field": f, "value": v,
                            "meta": meta})
    out.sort(key=lambda e: (e["name"], e["field"], e["value"]))
    return out


def _match_narrow(narrow, have, join="and"):
    """作品在收窄字段上是否还该出现。

    narrow: {字段: 允许的值集合}
    have:   {字段: 该作品实际拥有的值}
    join:   and -> 每个被收窄字段都要有值落在允许集合里
            or  -> 任一被收窄字段命中即可
    """
    if not narrow:
        return True
    hits = [any(v in allowed for v in have.get(f, []))
            for f, allowed in narrow.items()]
    return any(hits) if join == "or" else all(hits)


# ---------------------------------------------------------------- 分组目录

PATH_UNSAFE = str.maketrans({"/": "_", "\\": "_", ":": "_", "*": "_",
                             "?": "_", '"': "_", "<": "_", ">": "_",
                             "|": "_", "\n": "_", "\r": "_", "\t": "_"})

# 单个字段最多扇出到多少个目录,防止异常数据炸出几百个目标
VALUE_FANOUT_LIMIT = 12


def group_subdir(meta_row, group_by, fallback="未分类"):
    """按字段值算子目录名。

    meta_row: 含字段值的 dict(数据库行)
    group_by: 字段名,空串表示不建子目录
    """
    if not group_by:
        return ""
    v = (meta_row or {}).get(group_by)
    if v is None:
        return fallback
    s = str(v).strip()
    if not s:
        return fallback
    s = s.translate(PATH_UNSAFE).strip(". ")
    # 避免超长目录名
    if len(s) > 80:
        s = s[:80].strip(". ")
    return s or fallback


def distinct_values(db_path=DEFAULT_DB, field="artist", limit=500, names=None):
    """某字段的去重值列表(GUI 下拉框用),按出现次数降序。

    names: 只看这批文件名(None=全库; 空列表=一条都不给)。浏览页的
           下拉框跟着"本次扫描到的"走, 才不会列出当前范围里没有的值。
    """
    valid = set(META_COLUMNS) | {"language", "category"}
    if field not in valid:
        raise ValueError(f"非法字段: {field}")
    conn = sqlite3.connect(db_path)
    try:
        scope = scope_clause(conn, names)
        # 读 meta_values(拆分 + 过滤后的索引表), 不读 meta 表的原始列。
        # 否则下拉框会把已被当作空的值列出来 —— 比如原作里的 original,
        # 分类时它走「未分类」, 下拉框却还列着, 两处自相矛盾;
        # 而且原始列是多值逗号串, 拿它做等值筛选也匹配不上。
        rows = conn.execute(
            "SELECT mv.value, COUNT(DISTINCT mv.archive_id) FROM meta_values mv "
            "JOIN archives a ON a.id = mv.archive_id "
            "WHERE mv.field = ? AND mv.value IS NOT NULL AND mv.value != '' "
            f"AND ({scope}) "
            f"GROUP BY mv.value ORDER BY 2 DESC LIMIT {int(limit)}",
            (field,)).fetchall()
        return rows
    finally:
        conn.close()


# ---------------------------------------------------------------- CLI

def main():
    ap = argparse.ArgumentParser(description="漫画压缩包元数据采集器(本地)")
    ap.add_argument("--dir", action="append", default=None,
                    help="扫描目录,可多次指定")
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--rescan", action="store_true", help="强制重扫")
    args = ap.parse_args()
    dirs = args.dir or DEFAULT_SCAN_DIRS
    print(f"扫描 {dirs} -> {args.db}")
    stats = scan_dirs(dirs, args.db)
    for u in stats["unsupported"][:10]:
        print(f"  [不解析] {u}")
    if len(stats["unsupported"]) > 10:
        print(f"  ... 另有 {len(stats['unsupported'])-10} 个")


if __name__ == "__main__":
    main()

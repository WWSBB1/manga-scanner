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
    - 上次成功且有归一化元数据,且文件 size+mtime 未变 -> 跳过
    - 读过但没解析出元数据 -> 下次重读(下载器可能后补)
    - 上次报错 -> 下次重试
    - 同名不同目录 -> 告警跳过,不覆盖

本模块同时是 GUI 的后端库: scan_dirs() / list_archives() / filter_archives()
都可直接 import 使用。命令行入口: python3 scanner.py [--dir ...] [--db ...]
"""

import argparse
import json
import os
import sqlite3
import sys
import time
import xml.etree.ElementTree as ET
import zipfile

# ---------------------------------------------------------------- 配置

DEFAULT_SCAN_DIRS = ["/storage/emulated/0/Download"]
DEFAULT_DB = "manga.db"

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
             "LanguageISO", "PageCount", "Web", "Year", "Month", "Day"}


# ---------------------------------------------------------------- 小工具

def _first(*vals):
    for v in vals:
        if v not in (None, "", [], {}):
            return v
    return None


def _join(v):
    if v is None:
        return None
    if isinstance(v, (list, tuple)):
        items = [str(x).strip() for x in v if str(x).strip()]
        return ", ".join(items) or None
    s = str(v).strip()
    return s or None


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


def split_values(raw):
    """把一个字段的原始值拆成多个独立值。

    分隔符是逗号(兼容全角),去空白、去重、丢弃空项。
    None / 空串 / 纯空白都返回空列表。
    """
    if raw is None:
        return []
    s = str(raw)
    for sep in _VALUE_SEPS[1:]:
        s = s.replace(sep, _VALUE_SEPS[0])
    out, seen = [], set()
    for part in s.split(_VALUE_SEPS[0]):
        v = part.strip()
        if v and v not in seen:
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
    """把 'zh' / ['translated','chinese'] / 'chinese' 统一成 'chinese'。"""
    if isinstance(v, (list, tuple)):
        for item in v:
            s = str(item).strip().lower()
            if s and s != "translated":
                return LANG_MAP.get(s, s)
        return None
    if not v:
        return None
    s = str(v).strip().lower()
    return LANG_MAP.get(s, s)


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
    return _canon(
        title=gi.get("title"),
        title_original=gi.get("subtitle"),
        link=gi.get("URL") or gi.get("url"),
        category=gi.get("category"),
        tags=gi,
        language=gi.get("language"),
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
        "translated": 1 if pick("translated") else 0,
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

def scan_one(path):
    """扫一个压缩包,返回记录 dict。异常由调用方捕获。"""
    size = os.path.getsize(path)
    if not size:
        raise RuntimeError("文件大小为 0")
    with zipfile.ZipFile(path) as zf:
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

    rec = {
        "name": os.path.basename(path),
        "path": path,
        "size": size,
        "mtime": int(os.path.getmtime(path)),
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
    path          TEXT NOT NULL,
    size          INTEGER,
    mtime         INTEGER,
    entry_count   INTEGER,
    image_count   INTEGER,
    comicinfo_xml TEXT,                   -- 原文
    info_json     TEXT,
    meta_json     TEXT,
    metadata_json TEXT,
    extra_json    TEXT,                   -- 其它元数据文件 {文件名: 原文}
    has_meta      INTEGER DEFAULT 0,      -- 1=有归一化元数据;0=下次还要重读
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

ARCHIVE_COLUMNS = ["name", "path", "size", "mtime", "entry_count", "image_count",
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
    migrate_meta_values(conn)
    conn.commit()
    return conn


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
        for v in split_values(meta_row.get(field)):
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
    """{name: (path, size, mtime, has_meta, error)}"""
    return {n: (p, s, m, h, e) for n, p, s, m, h, e in conn.execute(
        "SELECT name, path, size, mtime, has_meta, error FROM archives")}


# ---------------------------------------------------------------- 主扫描流程

def scan_dirs(dirs, db_path=DEFAULT_DB, log=print, rescan=False,
              progress=None, stop_flag=None):
    """扫描目录列表,增量入库。返回统计 dict。

    log:        每行日志的回调(GUI 里接 UI)
    progress:   (done, total) 回调
    stop_flag:  可变对象,真值即中止(GUI 停止按钮)
    """
    def emit(msg):
        log(msg)

    targets, unsupported = [], []
    for root in dirs:
        if not os.path.isdir(root):
            emit(f"⚠ 目录不存在: {root}")
            continue
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames.sort()
            for fn in sorted(filenames):
                ext = os.path.splitext(fn)[1].lower()
                full = os.path.join(dirpath, fn)
                if ext in SUPPORTED:
                    targets.append(full)
                elif ext in NOTICE_EXT:
                    unsupported.append(full)
    emit(f"发现 {len(targets)} 个可解析压缩包"
         + (f",另 {len(unsupported)} 个 rar/7z 不解析" if unsupported else ""))

    conn = open_db(db_path)
    try:
        existing = {} if rescan else load_existing(conn)
        todo, skip_n, conflict_n = [], 0, 0
        for path in targets:
            name = os.path.basename(path)
            prev = existing.get(name)
            if prev is not None:
                prev_path, prev_size, prev_mtime, prev_meta, prev_err = prev
                if prev_path != path:
                    conflict_n += 1
                    emit(f"⚠ 同名冲突,跳过: {name}\n  已入库: {prev_path}\n  本次:   {path}")
                    continue
                try:
                    st_size, st_mtime = os.path.getsize(path), int(os.path.getmtime(path))
                except OSError:
                    continue
                if (prev_err is None and prev_meta
                        and prev_size == st_size and prev_mtime == prev_mtime):
                    skip_n += 1
                    continue
            todo.append(path)
        emit(f"待处理 {len(todo)},增量跳过 {skip_n},同名冲突 {conflict_n}")

        done = fail = 0
        failures = []
        t0 = time.time()
        for i, path in enumerate(todo, 1):
            if stop_flag is not None and stop_flag():
                emit("⏹ 用户中止")
                break
            name = os.path.basename(path)
            try:
                rec = scan_one(path)
                save(conn, rec)
                if not rec["has_meta"]:
                    emit(f"  ○ {name[:70]} (无元数据,下次将重读)")
                done += 1
            except Exception as e:
                fail += 1
                failures.append((name, f"{type(e).__name__}: {e}"))
                _upsert(conn, "archives", "name", {
                    "name": name, "path": path,
                    "scanned_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "error": f"{type(e).__name__}: {e}",
                })
                emit(f"  ✗ {name[:70]}: {e}")
            if i % 10 == 0 or i == len(todo):
                conn.commit()
            if progress:
                progress(i, len(todo))
        conn.commit()
        el = time.time() - t0
        emit(f"完成: 成功 {done},失败 {fail},跳过 {skip_n},耗时 {el:.1f}s")
        if failures:
            emit(f"失败 {len(failures)} 个(下次自动重试)")
        return {"total": len(targets), "todo": len(todo), "done": done,
                "fail": fail, "skip": skip_n, "conflict": conflict_n,
                "unsupported": unsupported, "elapsed": el}
    finally:
        conn.close()


# ---------------------------------------------------------------- 查询(GUI 用)

def list_archives(db_path=DEFAULT_DB, where="1=1", args=(), limit=2000):
    """archives JOIN meta,返回字段名列表 + 行列表。"""
    conn = sqlite3.connect(db_path)
    try:
        cols = ["id", "name", "path", "size", "image_count", "has_meta",
                "error"] + META_COLUMNS
        sql = ("SELECT a." + ",".join(
                   c if c in ("id", "name", "path", "size", "image_count",
                              "has_meta", "error") else f"m.{c}"
                   for c in cols)
               + " FROM archives a LEFT JOIN meta m ON m.archive_id=a.id "
                 f"WHERE {where} ORDER BY a.name LIMIT {int(limit)}")
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
VALID_OPS = {
    "eq": "等于", "ne": "不等于", "contains": "包含",
    "not_contains": "不包含", "empty": "为空", "not_empty": "不为空",
    "count_gte": "数量>=", "count_lte": "数量<=",
}
VALID_JOINS = ("and", "or")

# 数量类操作符(需要先按字段分组计数)
COUNT_OPS = ("count_gte", "count_lte")


def _to_int(v, what="阈值"):
    try:
        return int(str(v).strip())
    except (TypeError, ValueError):
        raise ValueError(f"非法{what}: {v!r}")


def count_groups(db_path, field, min_count=0, max_count=0,
                 base_conditions=None, join="and"):
    """按字段的拆分值分组计数,返回作品数达标的值列表。

    base_conditions: 先按这些条件圈定候选作品(数量条件与它们取交集)
    min_count / max_count: 0 表示不限;两个都是 0 时至少要求 1 个作品
    """
    if field not in VALID_FIELDS:
        raise ValueError(f"非法字段: {field}")
    lo = _to_int(min_count, "下限") if min_count else 1
    hi = _to_int(max_count, "上限") if max_count else 0
    where, args = build_where(base_conditions or [], join)
    conn = sqlite3.connect(db_path)
    try:
        sql = ("SELECT mv.value, COUNT(DISTINCT mv.archive_id) c "
               "FROM meta_values mv JOIN archives a ON a.id=mv.archive_id "
               f"WHERE mv.field=? AND ({where}) "
               "GROUP BY mv.value")
        params = [field] + list(args)
        if hi:
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
    glue = f" {join.upper()} "
    return glue.join(parts), args


def query_archives(db_path=DEFAULT_DB, conditions=None, join="and", limit=2000):
    """按多条件查询,返回 (cols, rows)。

    数量类条件(count_gte / count_lte)先分组计数算出达标的值,
    再把这些值当成等值条件并入查询。

    计数范围是**全部作品**,不受普通条件限制——否则 `作者数量>=5 OR
    语言=日语` 会先在日语作品里数作者,结果为空,整个 OR 就废了。
    普通条件照常通过 join 拼接参与,语义是"先各自算集合,再按 join 合并"。
    多个数量条件之间是交集(用户明确说这种情况该自己规避,不做拦截)。
    """
    conditions = list(conditions or [])
    normal = [c for c in conditions if (c.get("op") or "eq") not in COUNT_OPS]
    counts = [c for c in conditions if (c.get("op") or "eq") in COUNT_OPS]
    if not counts:
        where, args = build_where(normal, join)
        return list_archives(db_path, where, args, limit)

    # 每个数量条件: 在全部作品范围内分组计数
    extras = []
    for c in counts:
        v = _to_int(c.get("value") or 0)
        lo = v if c.get("op") == "count_gte" else 0
        hi = v if c.get("op") == "count_lte" else 0
        vals = count_groups(db_path, c["field"], min_count=lo, max_count=hi)
        if not vals:
            # 这个数量条件没有任何达标值, 单独看它是空集
            if join == "and":
                return list_archives(db_path, "1=0", [], limit)
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
    return list_archives(db_path, where, args, limit)


# ---------------------------------------------------------------- 分组目录

PATH_UNSAFE = str.maketrans({"/": "_", "\\": "_", ":": "_", "*": "_",
                             "?": "_", '"': "_", "<": "_", ">": "_",
                             "|": "_", "\n": "_", "\r": "_", "\t": "_"})


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


def distinct_values(db_path=DEFAULT_DB, field="artist", limit=500):
    """某字段的去重值列表(GUI 下拉框用),按出现次数降序。"""
    valid = set(META_COLUMNS) | {"language", "category"}
    if field not in valid:
        raise ValueError(f"非法字段: {field}")
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            f"SELECT m.{field}, COUNT(*) FROM meta m "
            f"WHERE m.{field} IS NOT NULL AND m.{field} != '' "
            f"GROUP BY m.{field} ORDER BY 2 DESC LIMIT {int(limit)}").fetchall()
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

#!/usr/bin/env python3
"""漫画管理 APP (纯 HTML 版)

用 Python 内置 http.server 提供轻量 Web 界面,手机浏览器秒开。
后端逻辑(scanner.py / fileops.py)完全复用,不依赖 Flet。

启动: python3 app.py   浏览器开 http://127.0.0.1:8951
环境变量: MANGA_DB / MANGA_PORT
"""

import json
import os
import re
import sqlite3
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

BASE = os.path.dirname(os.path.abspath(__file__))
import sys
sys.path.insert(0, BASE)

import scanner
import fileops
import plugin_loader

DB_PATH = os.environ.get("MANGA_DB",
                        os.path.join(BASE, "manga.db"))
PORT = int(os.environ.get("MANGA_PORT", "8951"))

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

# 最近一次扫描扫到的文件名。浏览页默认只看这批 ——
# 库里可能堆着几万条历史记录, 但用户此刻关心的是"刚扫的这批"。
# 不用 scanned_at 判断: 增量跳过的记录不会刷新它, 靠它筛不出"本次扫到"。
LAST_SCAN = {"names": None, "dirs": [], "at": None}


def fmt_size(n):
    if not n:
        return ""
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.0f}{unit}" if unit == "B" else f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}TB"


# ================================================================ API

class API:
    @staticmethod
    def scan_start(dirs, db, rescan, recursive=True):
        sid = f"scan_{int(time.time())}"
        SCAN_WORKERS[sid] = {"running": True, "stop": False,
                             "log": [], "stats": None}
        w = SCAN_WORKERS[sid]

        def log_fn(msg):
            w["log"].append(msg)

        def progress(done, total):
            pass  # 日志里已有进度

        def worker():
            try:
                stats = scanner.scan_dirs(
                    dirs, db, log=log_fn, rescan=rescan,
                    progress=progress,
                    stop_flag=lambda: w["stop"], recursive=recursive)
                w["stats"] = stats
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
                "stats": w["stats"]}

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
            vals = scanner.distinct_values(DB_PATH, field,
                                           names=API.scope_names(scope))
            return {"values": [{"value": v, "count": c} for v, c in vals]}
        except Exception as ex:
            return {"error": str(ex)}

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
                    cnt.setdefault(aid, {})[f] = n
                name_to_id = dict(conn.execute("SELECT name, id FROM archives"))
            finally:
                conn.close()
            counts = []
            for r in rows:
                d = dict(zip(cols, r))
                counts.append(cnt.get(name_to_id.get(d.get("name")), {}))
            return {"columns": cols, "rows": rows, "count": len(rows),
                    "value_counts": counts}
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
        """列出源目录下已入库的压缩包(库里没有的忽略)。"""
        try:
            items = fileops.match_in_db(DB_PATH, src_dir, recursive)
            return {"items": items, "count": len(items)}
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
            if not os.path.isdir(src_dir):
                return {"error": f"源目录不存在: {src_dir}"}
            # 先拿到源目录里都有哪些文件, 再让查询**在 SQL 层**按这个
            # 名单收窄。顺序不能反: 查询有 limit(按 name 排序取前 N),
            # 先查全库再过滤的话, 名字排在后面的目标会被 limit 截掉,
            # 界面就报"没有可分类的"。
            paths = {i["name"]: i["src"] for i in
                     fileops.match_in_db(DB_PATH, src_dir, recursive)}
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
            # exists_src 全 False, 预览会提示"找不到", 这是诚实的
            plan = fileops.plan_transfer(DB_PATH, names, base_dir, group_by,
                                         entries=by_name, group_dir=group_dir,
                                         paths=paths,
                                         fallback_names=fallback)
            # 预览按条目展开: 一个作品多个值就是多行
            rows = []
            for p in plan:
                for t in p["targets"]:
                    rows.append({"name": p["name"], "src": p["src"],
                                 "dest_dir": t,
                                 "dest": os.path.join(t, p["name"]),
                                 "targets": p["targets"],
                                 "exists_src": p["exists_src"]})
            return {"plan": plan, "rows": rows, "count": len(plan),
                    "entry_count": len(rows),
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
                if not os.path.isdir(src_dir):
                    w["result"] = {"error": f"源目录不存在: {src_dir}"}
                    return
                paths = {i["name"]: i["src"] for i in
                         fileops.match_in_db(DB_PATH, src_dir, recursive)}
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
                if approved:
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
                    fallback_names=fallback)
                w["result"] = result
                w["errors"] = errs
            except Exception as ex:
                w["result"] = {"error": str(ex)}
            finally:
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
    def stats():
        """总览: 各字段的基数概览(不含明细,明细走 /api/stat_field)。"""
        try:
            conn = sqlite3.connect(DB_PATH)
            try:
                total = conn.execute(
                    "SELECT COUNT(*) FROM archives").fetchone()[0]
                with_meta = conn.execute(
                    "SELECT COUNT(*) FROM archives WHERE has_meta=1"
                    ).fetchone()[0]
                errors = conn.execute(
                    "SELECT COUNT(*) FROM archives WHERE error IS NOT NULL"
                    ).fetchone()[0]
                # 各字段: 不同值个数 + 条目总数, 供字段选择器显示规模。
                # 只统计有分类意义的字段(见 scanner.STAT_FIELDS)
                fields = []
                for f in scanner.STAT_FIELDS:
                    n = conn.execute(
                        "SELECT COUNT(DISTINCT value) FROM meta_values "
                        "WHERE field=?", (f,)).fetchone()[0]
                    m = conn.execute(
                        "SELECT COUNT(*) FROM meta_values WHERE field=?",
                        (f,)).fetchone()[0]
                    if n:
                        fields.append({"field": f, "distinct": n,
                                       "entries": m})
                fields.sort(key=lambda x: -x["distinct"])
                avg_tags = 0
                if total:
                    te = conn.execute(
                        "SELECT COUNT(*) FROM meta_values WHERE field='tags'"
                        ).fetchone()[0]
                    avg_tags = round(te / total, 1)
            finally:
                conn.close()
            return {"total": total, "with_meta": with_meta,
                    "errors": errors, "fields": fields,
                    "avg_tags": avg_tags}
        except Exception as ex:
            return {"error": str(ex)}

    @staticmethod
    def stat_field(field, limit=20, offset=0, q="", sort="count"):
        """某字段的分布明细, 分页 + 搜索。

        limit/offset: 分页, 避免一次拉几万行
        q:            过滤值(子串匹配)
        sort:         count(按作品数) | name(按名称)
        返回 top(当前页) + total_values(该字段不同值总数) +
             tail(剩余值的数量和作品数, 用于显示"其他 N 项")
        """
        if field not in scanner.STAT_FIELDS:
            return {"error": f"非法字段: {field}"}
        try:
            conn = sqlite3.connect(DB_PATH)
            try:
                where, args = "field=?", [field]
                if q:
                    where += " AND value LIKE ?"
                    args.append(f"%{q}%")
                total_values = conn.execute(
                    f"SELECT COUNT(DISTINCT value) FROM meta_values "
                    f"WHERE {where}", args).fetchone()[0]
                order = "value ASC" if sort == "name" else "c DESC, value ASC"
                rows = conn.execute(
                    f"SELECT value, COUNT(DISTINCT archive_id) c "
                    f"FROM meta_values WHERE {where} GROUP BY value "
                    f"ORDER BY {order} LIMIT ? OFFSET ?",
                    args + [limit, offset]).fetchall()
                # 本级之外还剩多少(长尾折叠)
                tail = conn.execute(
                    f"SELECT COUNT(*), COALESCE(SUM(c),0) FROM ("
                    f"  SELECT COUNT(DISTINCT archive_id) c FROM meta_values "
                    f"  WHERE {where} GROUP BY value "
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
.dest-path{font-size:11px;color:#2e7d32;word-break:break-all}
/* 文件名完整显示不截断, 长名字靠换行容纳 */
#cls-table td:first-child{word-break:break-all}
.hl{background:#fff9c4;padding:0 2px;border-radius:2px}
.badge.mv{cursor:pointer;background:#7e57c2}
.vlist{margin-top:4px;line-height:1.8}
.vlist .tag{font-size:11px}

/* 详情弹窗: 默认隐藏(#detail-modal 没有 .show 时不显示) */
#detail-modal{position:fixed;inset:0;background:rgba(0,0,0,.5);
  z-index:200;display:none;align-items:center;justify-content:center}
#detail-modal.show{display:flex}
#detail-modal>.card{width:90%;max-width:500px;max-height:80vh;overflow-y:auto}
</style>
</head>
<body>

<div class="content" id="app">

<!-- ===== 扫描页 ===== -->
<div id="page-scan" class="page">
<h2>📦 扫描漫画</h2>
<div class="card">
  <label>扫描目录 (逗号分隔多个)</label>
  <input type="text" id="scan-dir" value="/storage/emulated/0/Download">
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
  </div>
  <div class="progress hidden" id="scan-prog"><div class="progress-fill" style="width:0"></div></div>
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
           oninput="syncDest()">
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
         oninput="destTouched=true">
  <label style="display:block;margin-top:8px;font-size:12px;color:#666">按字段建子目录</label>
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
    <label><input type="radio" name="mode" value="copy"> 复制</label>
    <label><input type="radio" name="mode" value="move" checked> 移动</label>
    <label><input type="checkbox" id="cls-preview" checked> 先预览</label>
  </div>
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
<div class="card" id="stats-overview"><button onclick="loadStats()">刷新统计</button></div>

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

</div>

<!-- 详情弹窗(默认隐藏, 点文件名才弹出) -->
<div id="detail-modal" onclick="if(event.target===this)closeDetail()">
  <div class="card">
    <div class="row" style="justify-content:space-between">
      <b>详情</b>
      <button class="outline" onclick="closeDetail()">关闭</button>
    </div>
    <div id="detail-content" style="margin-top:8px"></div>
  </div>
</div>

<!-- 底部导航 -->
<nav class="nav">
  <a onclick="showPage('scan')" id="nav-scan"><span class="icon">📦</span>扫描</a>
  <a onclick="showPage('browse')" id="nav-browse"><span class="icon">🔍</span>浏览</a>
  <a onclick="showPage('classify')" id="nav-classify"><span class="icon">📂</span>分类</a>
  <a onclick="showPage('stats')" id="nav-stats"><span class="icon">📊</span>统计</a>
</nav>

<script>
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
  // 统计页首次进入自动加载, 不用手动点刷新
  if(p==='stats'&&!statsLoaded)loadStats();
  // 浏览页同理: 进来就按当前范围(默认"本次扫描到的")显示
  if(p==='browse'){loadScopeHint();loadDistinct();doQuery();}
}
showPage('scan');

// ---- 扫描 ----
function doScan(){
  const dirs=document.getElementById('scan-dir').value;
  const db=document.getElementById('scan-db').value;
  const rescan=document.getElementById('rescan').checked;
  const recursive=document.getElementById('scan-recursive').checked;
  fetch('/api/scan',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({dirs,db,rescan,recursive})})
    .then(r=>r.json()).then(d=>{
      scanId=d.scan_id;
      document.getElementById('scan-log').textContent='';
      document.getElementById('scan-status').textContent='扫描中...';
      document.getElementById('scan-prog').classList.remove('hidden');
      document.getElementById('btn-stop').disabled=false;
      pollScan();
    });
}
function doStop(){
  fetch('/api/scan/stop?id='+scanId).then(r=>r.json());
}
function pollScan(){
  if(pollTimer)clearInterval(pollTimer);
  pollTimer=setInterval(()=>{
    fetch('/api/scan/status?id='+scanId).then(r=>r.json()).then(d=>{
      if(d.log)d.log.forEach(l=>{
        if(!document.getElementById('scan-log').textContent.includes(l))
          document.getElementById('scan-log').textContent+=l+'\n';
      });
      document.getElementById('scan-log').scrollTop=999999;
      if(!d.running){
        clearInterval(pollTimer);pollTimer=null;
        document.getElementById('btn-stop').disabled=true;
        document.getElementById('scan-prog').classList.add('hidden');
        if(d.stats){
          const s=d.stats;
          document.getElementById('scan-status').textContent=
            '完成 ✓ 成功'+s.done+' 失败'+s.fail+' 跳过'+s.skip+' 耗时'+(s.elapsed||0).toFixed(0)+'s';
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
    const sel=document.getElementById('browse-value');
    sel.innerHTML='<option value="">(全部)</option>'+
      d.values.map(v=>'<option value="'+esc(v.value)+'">'+esc(v.value)+' ('+v.count+')</option>').join('');
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
      document.getElementById('browse-count').textContent=d.count+' 条';
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

// ---- 详情 ----
function showDetail(name){
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
      html+='<div class="detail-pair"><div class="detail-key">'+
        (cnMap[k]||k)+'</div><div class="detail-val">'+esc(String(v))+'</div></div>';
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
}

function addCond(field,op,value){
  const id=++condSeq;
  const div=document.createElement('div');
  div.className='cond';div.id='cond-'+id;
  div.innerHTML='<span class="cond-idx">'+(document.getElementById('cls-conds').children.length+1)+'</span>'+
    '<select class="c-field">'+CLS_FIELDS.map(f=>
      '<option value="'+f[0]+'"'+(f[0]===(field||'artist')?' selected':'')+'>'+f[1]+'</option>').join('')+'</select>'+
    '<select class="c-op">'+CLS_OPS.map(o=>
      '<option value="'+o[0]+'"'+(o[0]===(op||'eq')?' selected':'')+'>'+o[1]+'</option>').join('')+'</select>'+
    '<input type="text" class="c-val" placeholder="值" value="'+esc(value||'')+'">'+
    '<button class="del" onclick="delCond('+id+')">×</button>';
  document.getElementById('cls-conds').appendChild(div);
  // 操作符切换时更新输入框提示; 数量条件和"为空/不为空"的输入框行为不同
  const opSel=div.querySelector('.c-op'), valIn=div.querySelector('.c-val');
  function syncVal(){
    const o=opSel.value;
    valIn.placeholder=o.startsWith('count_')?'数字':'值';
    valIn.disabled=(o==='empty'||o==='not_empty');
    if(valIn.disabled)valIn.value='';
  }
  opSel.addEventListener('change',syncVal);
  syncVal();
  updateCondIdx();
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
      if(d.missing&&d.missing.length){
        miss.textContent='⚠ '+d.missing.length+' 个文件在磁盘上找不到,会被跳过';
      }else{miss.textContent='';}
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
  // 勾了"先预览"但还没预览过 -> 先预览
  if(document.getElementById('cls-preview').checked&&!previewNames){
    doPreview();return;
  }
  if(!lastPlan||!lastPlan.length){
    alert('没有要处理的文件,先预览一下');return;
  }
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
let statField=null, statShown=0, statTotal=0, statTimer=null, statsLoaded=false;
const STAT_PAGE=20, STAT_STEP=100;

function loadStats(){
  statsLoaded=true;
  fetch('/api/stats').then(r=>r.json()).then(d=>{
    const el=document.getElementById('stats-overview');
    if(d.error){el.innerHTML='<b>总览</b><p>'+esc(d.error)+'</p>';return;}
    el.innerHTML='<b>总览</b>'+
      '<p>压缩包总数: <b>'+d.total+'</b></p>'+
      '<p>有元数据: <b>'+d.with_meta+'</b> / '+d.total+'</p>'+
      '<p>读取失败: <b>'+d.errors+'</b></p>'+
      '<p>平均每个作品 <b>'+d.avg_tags+'</b> 个标签</p>'+
      '<p style="font-size:12px;color:#666;margin-top:6px">点下面按钮切换字段</p>';
    // 字段按钮
    const fe=document.getElementById('stats-fields');
    fe.innerHTML=(d.fields||[]).map(f=>
      '<button class="outline" onclick="pickStatField(\''+f.field+'\')">'+
      esc(FIELD_CN_STAT[f.field]||f.field)+
      ' <span style="font-size:11px;color:#888">'+f.distinct+'</span></button>').join('');
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
    offset:statShown,q:q,sort:sort}))
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
    offset:0,q:q,sort:sort}))
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
  // 统计数字是全库的, 下钻也必须查全库 —— 不传 scope 会默认成"本次扫描",
  // 扫描范围之外的作品就会显示 0 条, 跟上面那条分布条自相矛盾
  fetch('/api/query?'+new URLSearchParams({field:statField,value:val,
                                           contains:'',scope:'all'}))
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
        '<div style="font-size:12px;color:#666;margin-bottom:6px">'+d.count+' 个作品</div>'+
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
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _html(self, html):
        # 页面里的路径占位符换成实际值(DB 路径随运行位置变)
        html = html.replace("__DB_PATH__", _attr_escape(DB_PATH))
        body = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self):
        length = int(self.headers.get("Content-Length", 0))
        if length:
            return json.loads(self.rfile.read(length))
        return {}

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)

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
        if path == "/api/scope":
            n = LAST_SCAN["names"]
            self._json({"scan_count": (len(n) if n is not None else None),
                        "dirs": LAST_SCAN["dirs"], "at": LAST_SCAN["at"]})
            return
        if path == "/api/detail":
            name = qs.get("name", [""])[0]
            self._json(API.detail(name))
            return
        if path == "/api/scan/status":
            sid = qs.get("id", [""])[0]
            self._json(API.scan_status(sid))
            return
        if path == "/api/scan/stop":
            sid = qs.get("id", [""])[0]
            self._json(API.scan_stop(sid))
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
            self._json(API.stats())
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
            self._json(API.stat_field(field, limit, offset, q, sort))
            return
        if path == "/api/plugins":
            self._json(API.plugins())
            return
        self.send_error(404)

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path
        try:
            body = self._read_body()
        except Exception as e:
            self._json({"error": f"请求体解析失败: {e}"}, 400)
            return

        if path == "/api/scan":
            dirs = [d.strip() for d in body.get("dirs", "").split(",") if d.strip()]
            db = body.get("db", DB_PATH)
            rescan = body.get("rescan", False)
            recursive = body.get("recursive", False)   # 界面默认不递归
            if not dirs:
                self._json({"error": "请填目录"})
                return
            self._json(API.scan_start(dirs, db, rescan, recursive))
            return
        if path == "/api/classify":
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
        if path == "/api/source":
            src_dir = body.get("src_dir", "")
            recursive = body.get("recursive", False)
            if not src_dir:
                self._json({"error": "请填源目录"})
                return
            self._json(API.source_list(src_dir, recursive))
            return
        self.send_error(404)

    def log_message(self, *args):
        pass  # 静默


def main():
    print(f"漫画管理 APP")
    print(f"数据库: {DB_PATH}")
    print(f"浏览器打开: http://127.0.0.1:{PORT}")
    print(f"Ctrl+C 停止\n")
    server = HTTPServer(("0.0.0.0", PORT), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n已停止")
        server.shutdown()


if __name__ == "__main__":
    main()

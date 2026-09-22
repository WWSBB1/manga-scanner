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

DB_PATH = os.environ.get("MANGA_DB", "manga.db")
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
            "error": "错误", "scanned_at": "扫描时间", "path": "路径",
            "entry_count": "条目数", "id": "ID", "archive_id": "归档ID"}

SCAN_WORKERS = {}   # scan_id -> {running, stop, log[], stats}
CLS_WORKERS = {}    # cls_id -> {running, stop, log[], result}


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
    def scan_start(dirs, db, rescan):
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
                    stop_flag=lambda: w["stop"])
                w["stats"] = stats
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
    def distinct(field):
        try:
            vals = scanner.distinct_values(DB_PATH, field)
            return {"values": [{"value": v, "count": c} for v, c in vals]}
        except Exception as ex:
            return {"error": str(ex)}

    @staticmethod
    def query(field, value, contains, limit=2000):
        where, args = "1=1", []
        if contains and field:
            where, args = f"m.{field} LIKE ?", [f"%{contains}%"]
        elif value and field:
            where, args = f"m.{field} = ?", [value]
        try:
            cols, rows = scanner.list_archives(DB_PATH, where, args, limit)
            return {"columns": cols, "rows": rows, "count": len(rows)}
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
                       recursive=False, limit=5000):
        """按条件算出条目 + 目标路径,不碰文件。

        条目级: 一个作品的多值字段会展开成多个条目, 每个条目独立判断,
        筛选后剩下的值直接决定分类目录。
        """
        try:
            entries = scanner.query_entries(DB_PATH, conditions, join,
                                            fields=[group_by], limit=limit)
            if src_dir:
                allowed = {i["name"] for i in
                           fileops.match_in_db(DB_PATH, src_dir, recursive)}
                entries = [e for e in entries if e["name"] in allowed]
            # 按作品聚合成 {name: [值...]}
            by_name = {}
            for e in entries:
                by_name.setdefault(e["name"], [])
                if e["value"] not in by_name[e["name"]]:
                    by_name[e["name"]].append(e["value"])
            names = list(by_name)
            plan = fileops.plan_transfer(DB_PATH, names, base_dir, group_by,
                                         entries=by_name)
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
    def classify_go(names, dest, mode, group_by="", src_dir=None,
                    recursive=False, conditions=None, join="and"):
        cid = f"cls_{int(time.time())}"
        w = {"running": True, "stop": False, "log": [], "result": None}
        CLS_WORKERS[cid] = w

        def log_fn(msg):
            w["log"].append(msg)

        def progress(i, total):
            pass

        def worker():
            try:
                entries = scanner.query_entries(
                    DB_PATH, conditions or [], join, fields=[group_by])
                if src_dir:
                    allowed = {i["name"] for i in
                               fileops.match_in_db(DB_PATH, src_dir, recursive)}
                    entries = [e for e in entries if e["name"] in allowed]
                by_name = {}
                for e in entries:
                    by_name.setdefault(e["name"], [])
                    if e["value"] not in by_name[e["name"]]:
                        by_name[e["name"]].append(e["value"])
                use_names = list(by_name)
                if not use_names:
                    log_fn("筛选后没有任何条目, 无事可做")
                result, errs = fileops.transfer_by_names(
                    DB_PATH, use_names, dest, mode,
                    log=log_fn, progress=progress,
                    stop_flag=lambda: w["stop"], group_by=group_by,
                    entries=by_name)
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
        try:
            conn = sqlite3.connect(DB_PATH)
            try:
                total = conn.execute(
                    "SELECT COUNT(*) FROM archives").fetchone()[0]
                with_meta = conn.execute(
                    "SELECT COUNT(*) FROM archives WHERE has_meta=1").fetchone()[0]
                errors = conn.execute(
                    "SELECT COUNT(*) FROM archives WHERE error IS NOT NULL"
                    ).fetchone()[0]
                langs = conn.execute(
                    "SELECT m.language, COUNT(*) FROM meta m "
                    "GROUP BY 1 ORDER BY 2 DESC LIMIT 10").fetchall()
                top_artists = conn.execute(
                    "SELECT m.artist, COUNT(*) FROM meta m "
                    "WHERE m.artist IS NOT NULL "
                    "GROUP BY 1 ORDER BY 2 DESC LIMIT 15").fetchall()
                top_parodies = conn.execute(
                    "SELECT m.parody, COUNT(*) FROM meta m "
                    "WHERE m.parody IS NOT NULL "
                    "GROUP BY 1 ORDER BY 2 DESC LIMIT 10").fetchall()
            finally:
                conn.close()
            return {"total": total, "with_meta": with_meta,
                    "errors": errors,
                    "langs": [{"name": l or "未知", "count": c}
                              for l, c in langs],
                    "artists": [{"name": a, "count": c}
                                for a, c in top_artists],
                    "parodies": [{"name": p, "count": c}
                                 for p, c in top_parodies]}
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
.hl{background:#fff9c4;padding:0 2px;border-radius:2px}

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
  <input type="text" id="scan-db" value="manga.db">
  <div class="row" style="margin-top:8px">
    <label><input type="checkbox" id="rescan"> 强制重扫</label>
    <button onclick="doScan()">开始扫描</button>
    <button class="outline" id="btn-stop" onclick="doStop()" disabled>停止</button>
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
  <select id="cls-groupby">
    <option value="artist">按作者</option>
    <option value="parody">按原作</option>
    <option value="group_name">按社团</option>
    <option value="language">按语言</option>
    <option value="character">按角色</option>
    <option value="category">按分类</option>
  </select>
  <div style="font-size:11px;color:#888;margin-top:4px">
    结果: 基础目录/字段值/文件.zip,取不到值的放「未分类」
  </div>
</div>

<div class="card">
  <b>4. 执行</b>
  <div class="row" style="margin-top:8px">
    <label><input type="radio" name="mode" value="copy" checked> 复制</label>
    <label><input type="radio" name="mode" value="move"> 移动</label>
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
<div class="card"><button onclick="loadStats()">刷新统计</button></div>
<div id="stats-content"></div>
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

function showPage(p){
  document.querySelectorAll('.page').forEach(e=>e.classList.add('hidden'));
  document.getElementById('page-'+p).classList.remove('hidden');
  document.querySelectorAll('.nav a').forEach(a=>a.classList.remove('active'));
  document.getElementById('nav-'+p).classList.add('active');
}
showPage('scan');

// ---- 扫描 ----
function doScan(){
  const dirs=document.getElementById('scan-dir').value;
  const db=document.getElementById('scan-db').value;
  const rescan=document.getElementById('rescan').checked;
  fetch('/api/scan',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({dirs,db,rescan})})
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
function loadDistinct(){
  const f=document.getElementById('browse-field').value;
  fetch('/api/distinct?field='+f).then(r=>r.json()).then(d=>{
    const sel=document.getElementById('browse-value');
    sel.innerHTML='<option value="">(全部)</option>'+
      d.values.map(v=>'<option value="'+esc(v.value)+'">'+esc(v.value)+' ('+v.count+')</option>').join('');
  });
}
function doQuery(){
  const f=document.getElementById('browse-field').value;
  const v=document.getElementById('browse-value').value;
  const c=document.getElementById('browse-contains').value;
  fetch('/api/query?'+new URLSearchParams({field:f,value:v,contains:c}))
    .then(r=>r.json()).then(d=>{
      const tb=document.querySelector('#browse-table tbody');
      document.getElementById('browse-count').textContent=d.count+' 条';
      if(d.error){tb.innerHTML='<tr><td colspan=6>'+esc(d.error)+'</td></tr>';return;}
      tb.innerHTML=d.rows.map(r=>{
        const m=cols2obj(d.columns,r);
        return '<tr onclick="showDetail('+JSON.stringify(m.name)+')">'+
          '<td>'+esc((m.name||'').slice(0,40))+'</td>'+
          '<td>'+esc(m.artist||'')+'</td>'+
          '<td>'+esc(m.parody||'')+'</td>'+
          '<td>'+esc(m.language||'')+'</td>'+
          '<td>'+(m.page_count||'')+'</td>'+
          '<td>'+esc((m.tags||'').slice(0,50))+'</td></tr>';
      }).join('');
    });
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

// 目标基础目录默认跟随源目录; 用户手动改过就不再自动跟随
function syncDest(){
  if(destTouched)return;
  const s=document.getElementById('cls-src').value.trim();
  if(s)document.getElementById('cls-dest').value=s;
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
addCond();

// ---- 分类: 预览与执行 ----
function clsPayload(){
  return {
    conditions:collectConds(),
    join:document.getElementById('cls-join').value,
    base_dir:document.getElementById('cls-dest').value.trim(),
    group_by:document.getElementById('cls-groupby').value||'artist',
    src_dir:document.getElementById('cls-src').value.trim()||null,
    recursive:document.getElementById('cls-recursive').checked,
    mode:document.querySelector('input[name=mode]:checked').value
  };
}
function doPreview(){
  const p=clsPayload();
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
      const shortName=x=>(x.name||'').slice(0,38);
      tb.innerHTML=d.plan.map(x=>{
        const tg=(x.targets&&x.targets.length)?x.targets:[x.dest_dir];
        const subOf=t=>(t&&t!==base)?t.slice(base.length).replace(/^\//,''):'(基础目录)';
        let destHtml='<span class="hl">'+esc(subOf(tg[0]))+'</span>/'+esc(shortName(x));
        if(tg.length>1){
          destHtml+='<br>'+tg.slice(1).map(t=>
            '<span class="hl">'+esc(subOf(t))+'</span>/'+esc(shortName(x))).join('<br>');
          destHtml+=' <span class="badge">共'+tg.length+'份</span>';
        }
        const bad=x.exists_src?'':' style="opacity:.45"';
        return '<tr'+bad+'><td>'+esc((x.name||'').slice(0,40))+'</td>'+
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
  fetch('/api/classify',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({dest:p.base_dir,mode:p.mode,group_by:p.group_by,
      src_dir:p.src_dir,recursive:p.recursive,
      conditions:p.conditions,join:p.join})})
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
            '完成: '+(r.ok||0)+' 成功, '+(r.fail||0)+' 失败';
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
function loadStats(){
  fetch('/api/stats').then(r=>r.json()).then(d=>{
    const el=document.getElementById('stats-content');
    if(d.error){el.innerHTML='<div class="card">'+esc(d.error)+'</div>';return;}
    const maxCount=Math.max(...d.langs.map(l=>l.count),1);
    let html='<div class="card"><h3>总览</h3>';
    html+='<p>压缩包总数: <b>'+d.total+'</b></p>';
    html+='<p>有元数据: <b>'+d.with_meta+'</b> / '+d.total+'</p>';
    html+='<p>读取失败: <b>'+d.errors+'</b></p></div>';
    html+='<div class="card"><h3>语言分布</h3>';
    d.langs.forEach(l=>{
      html+='<div class="bar"><div class="bar-fill" style="width:'+(l.count/maxCount*100)+'%">'+l.name+' '+l.count+'</div></div>';
    });
    html+='</div>';
    html+='<div class="card"><h3>作者 Top15</h3>';
    const maxA=Math.max(...d.artists.map(a=>a.count),1);
    d.artists.forEach(a=>{
      html+='<div class="bar"><div class="bar-fill" style="width:'+(a.count/maxA*100)+'%">'+a.name+' '+a.count+'</div></div>';
    });
    html+='</div>';
    html+='<div class="card"><h3>原作分布</h3>';
    const maxP=Math.max(...d.parodies.map(p=>p.count),1);
    d.parodies.forEach(p=>{
      html+='<div class="bar"><div class="bar-fill" style="width:'+(p.count/maxP*100)+'%">'+p.name+' '+p.count+'</div></div>';
    });
    html+='</div>';
    el.innerHTML=html;
  });
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


class Handler(BaseHTTPRequestHandler):
    def _json(self, data, code=200):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _html(self, html):
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
            if field not in VALID_FIELDS:
                self._json({"error": f"非法字段: {field}"})
                return
            self._json(API.distinct(field))
            return
        if path == "/api/query":
            field = qs.get("field", [""])[0]
            value = qs.get("value", [""])[0]
            contains = qs.get("contains", [""])[0]
            if field and field not in VALID_FIELDS:
                self._json({"error": f"非法字段: {field}"})
                return
            self._json(API.query(field, value, contains))
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
            if not dirs:
                self._json({"error": "请填目录"})
                return
            self._json(API.scan_start(dirs, db, rescan))
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
            if not dest:
                self._json({"error": "缺少目标目录"})
                return
            if not group_by:
                self._json({"error": "请选择按哪个字段建子目录"})
                return
            self._json(API.classify_go(names, dest, mode, group_by,
                                       src_dir, recursive, conditions, join))
            return
        if path == "/api/preview":
            conditions = body.get("conditions", [])
            join = body.get("join", "and")
            base_dir = body.get("base_dir", "")
            group_by = body.get("group_by", "")
            src_dir = body.get("src_dir") or None
            recursive = body.get("recursive", False)
            if not base_dir:
                self._json({"error": "请填目标基础目录"})
                return
            if not group_by:
                self._json({"error": "请选择按哪个字段建子目录"})
                return
            self._json(API.filter_preview(conditions, join, base_dir,
                                          group_by, src_dir, recursive))
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

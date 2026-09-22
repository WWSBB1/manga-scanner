#!/usr/bin/env python3
"""文件操作: 对筛选出的压缩包执行移动/复制。

教训(来自之前的 rclone 教训): 返回码/无异常不代表成功,
每个文件操作后都核对目标存在且大小一致才计入成功。

sdcard 上没有 os.rename 跨文件系统的原子性保障,
统一走 复制+校验+删源(移动) 或 复制+校验(复制)。
"""

import os
import shutil
import sqlite3
import time

import scanner


def _unique_dest(dest):
    """目标已存在时改名 xxx (1).zip 避免覆盖。"""
    if not os.path.exists(dest):
        return dest
    base, ext = os.path.splitext(dest)
    for i in range(1, 1000):
        cand = f"{base} ({i}){ext}"
        if not os.path.exists(cand):
            return cand
    raise RuntimeError(f"无法生成不冲突的文件名: {dest}")


def _copy_verified(src, dest_dir, log=print):
    """复制到 dest_dir 并校验大小,不删源。返回 (成功?, 目标路径或错误)。

    目标目录不存在则创建;同名冲突自动改名不覆盖;失败时清理半成品。
    """
    name = os.path.basename(src)
    if not os.path.isfile(src):
        return False, f"源不存在: {src}"
    try:
        os.makedirs(dest_dir, exist_ok=True)
    except OSError as e:
        return False, f"建目录失败 {dest_dir}: {e}"
    dest = _unique_dest(os.path.join(dest_dir, name))
    src_size = os.path.getsize(src)
    try:
        shutil.copy2(src, dest)
    except Exception as e:
        try:
            if os.path.exists(dest):
                os.remove(dest)
        except OSError:
            pass
        return False, f"复制失败: {type(e).__name__}: {e}"
    try:
        dest_size = os.path.getsize(dest)
    except OSError as e:
        return False, f"校验失败(无法读目标): {e}"
    if dest_size != src_size:
        try:
            os.remove(dest)
        except OSError:
            pass
        return False, f"校验失败(大小 {dest_size} != {src_size})"
    return True, dest


def transfer_one(src, dest_dir, mode="copy", log=print):
    """移动/复制单个文件到单个目录。返回 (成功?, 目标路径或错误)。

    mode: copy | move
    完成后校验目标大小与源一致,移动模式校验通过才删源。
    """
    ok, msg = _copy_verified(src, dest_dir, log)
    if not ok:
        return False, msg
    if mode == "move":
        try:
            os.remove(src)
        except OSError as e:
            return False, (f"已复制到目标但删源失败(内容安全,源在): {e}")
        # 双保险: 删源后确认源真没了
        if os.path.exists(src):
            return False, "已复制但删源后源仍存在,请手动检查"
    return True, msg


def _load_rows(db_path, names):
    """按文件名取库里的记录(含 meta 字段,供分组用)。"""
    if not names:
        return []
    conn = sqlite3.connect(db_path)
    try:
        placeholders = ",".join("?" * len(names))
        cols = ["id", "name", "path"] + scanner.META_COLUMNS
        sql = ("SELECT a.id, a.name, a.path, "
               + ",".join(f"m.{c}" for c in scanner.META_COLUMNS)
               + " FROM archives a LEFT JOIN meta m ON m.archive_id=a.id "
                 f"WHERE a.name IN ({placeholders})")
        rows = conn.execute(sql, names).fetchall()
    finally:
        conn.close()
    return [dict(zip(cols, r)) for r in rows]


def plan_transfer(db_path, names, base_dir, group_by="", mode="copy"):
    """生成搬移计划(不碰文件),供预览和执行共用。

    多值字段会扇出: 一个作品在该字段有多个值就有多个目标目录。
    返回 [{name, src, targets:[目录...], dest_dir, dest, exists_src, error}]
    dest_dir / dest 是第一个目标,兼容只关心单目标的调用方。
    """
    rows = _load_rows(db_path, names)
    by_name = {r["name"]: r for r in rows}
    limit = scanner.VALUE_FANOUT_LIMIT
    plan = []
    for n in names:
        r = by_name.get(n)
        if not r:
            plan.append({"name": n, "src": None, "targets": [],
                         "dest_dir": None, "dest": None, "exists_src": False,
                         "error": "库里查不到"})
            continue
        if group_by:
            vals = scanner.split_values(r.get(group_by))[:limit] or ["未分类"]
            subs = []
            for v in vals:
                s = scanner.group_subdir({group_by: v}, group_by)
                if s not in subs:
                    subs.append(s)
            targets = [os.path.join(base_dir, s) for s in subs]
        else:
            targets = [base_dir]
        first = targets[0]
        plan.append({
            "name": n,
            "src": r.get("path"),
            "targets": targets,
            "dest_dir": first,
            "dest": os.path.join(first, n),
            "exists_src": os.path.isfile(r.get("path") or ""),
            "artist": r.get("artist"), "parody": r.get("parody"),
            "language": r.get("language"),
        })
    return plan


def match_in_db(db_path, src_dir, recursive=True):
    """列出源目录下的压缩包,只保留数据库里有记录的。

    返回 [{name, src, dest_dir(None), exists_src:True}, ...]
    数据库中没记录的直接忽略(不处理)。
    """
    exts = scanner.ARCHIVE_EXT
    found = []
    if not os.path.isdir(src_dir):
        return found
    if recursive:
        walker = os.walk(src_dir)
        for dirpath, dirnames, filenames in walker:
            dirnames.sort()
            for fn in sorted(filenames):
                if os.path.splitext(fn)[1].lower() in exts:
                    found.append((fn, os.path.join(dirpath, fn)))
    else:
        try:
            for fn in sorted(os.listdir(src_dir)):
                full = os.path.join(src_dir, fn)
                if (os.path.isfile(full)
                        and os.path.splitext(fn)[1].lower() in exts):
                    found.append((fn, full))
        except OSError:
            return []
    if not found:
        return []
    names = [n for n, _ in found]
    rows = _load_rows(db_path, names)
    in_db = {r["name"]: r for r in rows}
    out = []
    for n, path in found:
        r = in_db.get(n)
        if not r:
            continue                      # 库里没有 -> 忽略
        out.append({"name": n, "src": path, "dest_dir": None,
                    "dest": None, "exists_src": True,
                    "db_path": r.get("path"),
                    "artist": r.get("artist"), "parody": r.get("parody"),
                    "language": r.get("language")})
    return out


def transfer_by_names(db_path, names, dest_dir, mode="copy", log=print,
                      progress=None, stop_flag=None, group_by=""):
    """按文件名列表批量移动/复制。

    names:    archives.name 的列表
    dest_dir: 基础目标目录
    group_by: 按该字段建子目录(空串则全放 dest_dir)
    移动成功后把库里的 path 更新为新位置,让后续扫描不误判"同名冲突"。
    返回 {结果统计}, {name: 消息} 失败清单
    """
    if mode not in ("copy", "move"):
        raise ValueError(f"mode 必须是 copy/move: {mode}")
    plan = plan_transfer(db_path, names, dest_dir, group_by, mode)
    plan_by_name = {p["name"]: p for p in plan}

    ok = fail = 0
    errors = {}
    moved_paths = {}
    total = len(names)
    for i, n in enumerate(names, 1):
        if stop_flag is not None and stop_flag():
            log("⏹ 用户中止")
            break
        p = plan_by_name.get(n)
        if not p or p.get("error"):
            fail += 1
            errors[n] = (p or {}).get("error", "库里查不到路径")
            continue
        src = p["src"]
        if not src:
            fail += 1
            errors[n] = "库里查不到路径"
            continue
        if not os.path.isfile(src):
            fail += 1
            errors[n] = f"源文件不存在: {src}"
            log(f"  ✗ {n[:70]}: {errors[n]}")
            if progress:
                progress(i, total)
            continue
        targets = p["targets"]
        # 扇出: 先把除第一个外的目标都复制出来,再搬第一个(移动模式下删源)。
        # 顺序不可颠倒 —— 先移动会把源搬走,后面的副本就没源可读了。
        copy_failed = None
        for t in targets[1:]:
            try:
                cok, cmsg = _copy_verified(src, t, log)
            except Exception as e:
                cok, cmsg = False, f"{type(e).__name__}: {e}"
            if not cok:
                copy_failed = f"副本 {t} 失败: {cmsg}"
                break
        if copy_failed:
            # 副本没做全 -> 不动源文件,已做好的副本保留
            fail += 1
            errors[n] = copy_failed
            log(f"  ✗ {n[:70]}: {copy_failed}")
            if progress:
                progress(i, total)
            continue
        try:
            success, msg = transfer_one(src, targets[0], mode, log)
        except Exception as e:
            success, msg = False, f"{type(e).__name__}: {e}"
        if success:
            ok += 1
            moved_paths[n] = msg  # msg=新路径
            if len(targets) > 1:
                log(f"  ✓ {n[:60]} -> {len(targets)} 个目录")
        else:
            fail += 1
            errors[n] = msg
            log(f"  ✗ {n[:70]}: {msg}")
        if progress:
            progress(i, total)

    # 移动成功的更新库里的 path(核对过新路径确实存在才更新)
    if mode == "move" and moved_paths:
        conn = sqlite3.connect(db_path)
        try:
            for n, newp in moved_paths.items():
                if os.path.exists(newp):
                    conn.execute("UPDATE archives SET path=? WHERE name=?",
                                 (newp, n))
            conn.commit()
        finally:
            conn.close()

    log(f"完成: 成功 {ok},失败 {fail},目标 {dest_dir}")
    return {"ok": ok, "fail": fail, "total": total, "mode": mode,
            "dest": dest_dir}, errors

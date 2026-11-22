#!/usr/bin/env python3
"""文件操作: 对筛选出的压缩包执行移动/复制。

教训(来自之前的远程操作经验): 返回码/无异常不代表成功,
每个文件操作后都核对目标存在且大小一致才计入成功。

sdcard 上没有 os.rename 跨文件系统的原子性保障,
统一走 复制+校验+删源(移动) 或 复制+校验(复制)。
"""

import os
import shutil
import sqlite3
import time

import scanner
import source as _source


def _is_remote(path):
    """远程路径(123:/...)。单独包一层, 方便和 scanner 的口径统一。"""
    return _source.is_remote(path)


def _join(root, rel):
    return _source.join(root, rel)


def _rel_of(path, root):
    """完整路径 -> 相对数据源根的路径。

    远程 `123:/a/b.zip` + 根 `123:/a` -> `b.zip`。
    本地 `/a/b.zip` + 根 `/a` -> `b.zip`。
    """
    if _is_remote(root):
        prefix = root.rstrip("/") + "/"
        if path.startswith(prefix):
            return path[len(prefix):]
        return path
    rel = os.path.relpath(path, root)
    return rel


def _remote_transfer(source, src, targets, mode, log):
    """远程搬移: 只支持移动, 多值只进第一个目标。

    远程没有可用的服务端复制(123 WebDAV 的 COPY 返回 500),
    下载再上传又太慢。所以多值扇出时只搬到第一个目标目录,
    其余目标明确写进日志 —— 不能悄悄少搬, 也不能假装搬成功。
    """
    src_rel = _rel_of(src, source.root)
    first = targets[0]
    name = os.path.basename(src)
    # 目标目录通常不在源根下面(分类整理就是搬去别的目录), 所以这里
    # 直接拼完整远程路径, 交给 source.move 按完整路径处理。
    # 先明确创建本次实际使用的目标目录: 远程没有 os.makedirs, 而按
    # 标签/作者分类时目标就是「基础目录/字段名/值」。不依赖 moveto
    # 是否碰巧自动补父目录。
    source.ensure_dir(first)
    dst_rel = _source.join(first, name)
    status, msg = source.move(src_rel, dst_rel)
    if status == "ok":
        if len(targets) > 1:
            rest = list(targets[1:])
            log(f"  ⚠ {name[:50]}: 远程不支持复制, "
                f"只移到第一个目录; 另有 {len(rest)} 个目录未处理: "
                + ", ".join(rest[:3]) + (" …" if len(rest) > 3 else ""))
        return True, ""
    if status == "skip":
        return False, f"SKIP:{msg}"
    return False, msg


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

    目标目录不存在则创建。
    目标已存在同名文件 -> 跳过(不改名、不覆盖), 返回特殊标记供调用方识别。
    失败时清理半成品。
    返回 (False, "SKIP:...") 表示跳过而非失败。
    """
    name = os.path.basename(src)
    if not os.path.isfile(src):
        return False, f"源不存在: {src}"
    try:
        os.makedirs(dest_dir, exist_ok=True)
    except OSError as e:
        return False, f"建目录失败 {dest_dir}: {e}"
    dest = os.path.join(dest_dir, name)
    if os.path.exists(dest):
        # 目标已有同名文件: 跳过, 不产生 (1)(2) 副本, 也不覆盖
        return False, f"SKIP:目标已存在 {dest}"
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
    """移动/复制单个文件到单个目录。返回 (成功?, 目标路径或错误/跳过)。

    mode: copy | move
    目标已存在同名文件时跳过(不改名不覆盖), 返回 "SKIP:..." 消息 ——
    跳过时移动模式也不会删源文件。
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
        # 走临时表而不是 IN (?,?,...): 源目录可能有几万个文件, 拼成
        # 参数列表会撞上 SQLite 的变量个数上限, 直接抛 OperationalError,
        # 预览和分类整个失败。scanner.scope_clause 用的就是临时表。
        scope = scanner.scope_clause(conn, names)
        cols = ["id", "name"] + scanner.META_COLUMNS
        sql = ("SELECT a.id, a.name, "
               + ",".join(f"m.{c}" for c in scanner.META_COLUMNS)
               + " FROM archives a LEFT JOIN meta m ON m.archive_id=a.id "
                 f"WHERE {scope}")
        rows = conn.execute(sql).fetchall()
    finally:
        conn.close()
    return [dict(zip(cols, r)) for r in rows]


# 字段名 -> 建出来的中间层目录名
GROUP_DIR_CN = {
    "artist": "作者", "group_name": "社团", "parody": "原作",
    "character": "角色", "tags": "标签", "language": "语言",
    "category": "分类", "format": "格式",
}


def plan_transfer(db_path, names, base_dir, group_by="", mode="copy",
                  entries=None, group_dir=False, paths=None,
                  fallback_names=None, exists=None):
    """生成搬移计划(不碰文件),供预览和执行共用。

    多值字段会扇出: 一个作品在该字段有多个值就有多个目标目录。
    entries:   {name: [值, ...]},由筛选结果决定要进哪些值目录。
               不给(None)时沿用旧行为: 从 meta 读出该字段的全部值。
               值为空列表表示该作品没有保留任何值 -> 不产生目标。
    group_dir: True 时多包一层以字段命名的目录:
               基础目录/作者/zmix/x.zip 而不是 基础目录/zmix/x.zip。
               不同分类维度的结果就不会混在一起。
    paths:     {name: 绝对路径} —— 文件"当下在哪"。库不记路径, 所以搬移
               必须由调用方把本次找到的位置传进来(扫描结果或源目录扫描)。
               缺某个 name 就认为它这次找不到, exists_src=False。
    fallback_names: 这些作品在该字段上没有任何值(如原作只有 original 的
               原创作品), 应当落到「未分类」而不是被丢掉。与
               entries 里值为 [] 的含义不同 —— 后者是"筛选后没保留值,
               不该处理", 这里则是"本想按该字段归档, 但它没值"。
    exists:    判断"文件当下在不在"的回调, 默认 os.path.isfile。
               远程文件不能走 os.path.isfile, 由调用方传入
               `lambda p: source.get_source(root).exists(rel)` 之类的判断。
    返回 [{name, src, targets:[目录...], dest_dir, dest, exists_src, error}]
    dest_dir / dest 是第一个目标; targets 为空时两者都是 None。
    """
    fallback = set(fallback_names or ())
    paths = paths or {}
    if exists is None:
        exists = os.path.isfile
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
            if entries is not None:
                vals = list(entries.get(n) or [])
            else:
                # 传字段名, 让该字段的"等于没填"取值(如 parody 的 original)
                # 也在这里生效
                vals = scanner.split_values(r.get(group_by), group_by)
            # 兜底成「未分类」的两种情况:
            #   a) 旧行为(没给 entries): 从 meta 读不出值就未分类
            #   b) fallback_names: 这个作品在该字段上本来就没值
            # 其余"筛选后值为空"是有效结果 -> 不产生目标(不该处理它)
            if not vals:
                if entries is None or n in fallback:
                    vals = ["未分类"]
            # 扇出上限: 防异常数据炸出几百个目录。但**不能悄悄丢** ——
            # 库里实测有单个作品 34 个作者、50 个作品超过上限, 静默截断会让
            # 这些作品少归档一部分值, 用户还以为都处理了。记下丢了多少,
            # 由预览/日志明确报出来。
            dropped = list(vals[limit:])
            vals = vals[:limit]
            # 包一层字段名目录(如 作者/、标签/)
            root = base_dir
            if group_dir:
                root = _join(base_dir, GROUP_DIR_CN.get(group_by, group_by))
            subs = []
            seen_sub = {}           # 子目录名 -> 原始值(用于发现"两个不同值撞同名")
            merged = []             # 撞车于是被并进同一目录的值
            for v in vals:
                s = scanner.group_subdir({group_by: v}, group_by)
                if s not in subs:
                    subs.append(s)
                    seen_sub[s] = v
                elif seen_sub[s] != v:
                    # 两个**不同**的值清理后同名(如作者 "hiro" 与 "hiro.",
                    # 尾部点号被去掉)。它们会被并进同一个目录 —— 这可能是
                    # 想要的, 但必须说出来, 否则两个作者的作品悄悄混在一起
                    # 而没人知道。
                    merged.append((v, seen_sub[s]))
            targets = [_join(root, s) for s in subs]
        else:
            dropped = []
            merged = []
            targets = [base_dir]
        first = targets[0] if targets else None
        src = paths.get(n)
        plan.append({
            "name": n,
            "src": src,
            "targets": targets,
            "dest_dir": first,
            "dest": _join(first, n) if first else None,
            # 当场核对, 不是读库里可能过期的值
            "exists_src": bool(src) and exists(src),
            # 因扇出上限被丢掉的值(空列表=没丢)
            "dropped": dropped,
            # 清理后同名、被并进同一目录的不同值
            "merged": merged,
            "artist": r.get("artist"), "parody": r.get("parody"),
            "language": r.get("language"),
        })
    return plan


def match_in_db(db_path, src_dir, recursive=True, source=None):
    """列出源目录下的压缩包,只保留数据库里有记录的。

    这是"文件现在在哪"的主要来源 —— 库不记路径, 搬移前先用它把
    本次的位置扫出来({name: src}),再交给 plan_transfer。
    返回 [{name, src, dest_dir(None), exists_src:True, root, rel}, ...]
    数据库中没记录的直接忽略(不处理)。

    src_dir 可以是本地目录或 `123:/...` 远程路径; 远程由数据源层
    列目录, 不会走 os 函数。source 可显式传入(调用方复用以共享清单缓存)。
    """
    exts = scanner.ARCHIVE_EXT
    found = []
    if _is_remote(src_dir):
        src = source or _source.get_source(src_dir)
        # 远程文件不落地, 位置就是 `remote:/path` 字符串
        for rel, name, _size, _mtime in src.list_archives(recursive):
            found.append((name, _source.join(src_dir, rel), src, rel))
    else:
        if not os.path.isdir(src_dir):
            return found
        if recursive:
            walker = os.walk(src_dir)
            for dirpath, dirnames, filenames in walker:
                dirnames.sort()
                for fn in sorted(filenames):
                    if os.path.splitext(fn)[1].lower() in exts:
                        found.append((fn, os.path.join(dirpath, fn), None, None))
        else:
            try:
                for fn in sorted(os.listdir(src_dir)):
                    full = os.path.join(src_dir, fn)
                    if (os.path.isfile(full)
                            and os.path.splitext(fn)[1].lower() in exts):
                        found.append((fn, full, None, None))
            except OSError:
                return []
    if not found:
        return []
    names = [n for n, _p, _s, _r in found]
    rows = _load_rows(db_path, names)
    in_db = {r["name"]: r for r in rows}
    out = []
    for n, path, src, rel in found:
        r = in_db.get(n)
        if not r:
            continue                      # 库里没有 -> 忽略
        out.append({"name": n, "src": path, "dest_dir": None,
                    "dest": None, "exists_src": True,
                    "root": src_dir, "rel": rel,
                    "artist": r.get("artist"), "parody": r.get("parody"),
                    "language": r.get("language")})
    return out


def transfer_by_names(db_path, names, dest_dir, mode="copy", log=print,
                      progress=None, stop_flag=None, group_by="", entries=None,
                      group_dir=False, paths=None, fallback_names=None,
                      source=None):
    """按文件名列表批量移动/复制。

    names:    archives.name 的列表
    dest_dir: 基础目标目录
    group_by: 按该字段建子目录(空串则全放 dest_dir)
    entries:  {name: [值, ...]},由筛选结果决定要进哪些值目录(见 plan_transfer)
    group_dir: True 时多包一层字段名目录(基础目录/作者/值/文件)
    paths:    {name: 绝对路径},文件当下在哪。库不记路径, 必须由调用方提供。
    source:   数据源。给出且为远程时按远程能力处理; 不给时**完全走原有
              本地代码路径**(老调用零影响)。远程能力:
                * 只支持移动, 复制直接拒绝(123 WebDAV 的 COPY 返回 500)
                * 多值扇出只移到第一个目标, 其余目标在日志里说明
                * 目标已存在不覆盖
    返回 {结果统计}, {name: 消息} 失败清单
    """
    if mode not in ("copy", "move"):
        raise ValueError(f"mode 必须是 copy/move: {mode}")
    remote = source is not None and source.kind == "webdav-http"

    def _exists(path):
        if remote:
            return source.exists(_rel_of(path, source.root))
        return os.path.isfile(path)

    try:
        plan = plan_transfer(db_path, names, dest_dir, group_by, mode, entries,
                             group_dir, paths, fallback_names, exists=_exists)
    except _source.SourceError as e:
        # 连源清单都读不到: 一个文件都不能判断, 直接整批拒绝。
        # 绝不能带着"全都找不到"的半成品计划往下走 —— 移动不可逆。
        msg = f"读取远程文件清单失败, 未执行任何操作: {e}"
        log(f"✗ {msg}")
        return ({"ok": 0, "fail": len(names), "skip": 0, "total": len(names),
                 "mode": mode, "dest": dest_dir, "remote": remote,
                 "error": msg}, {n: msg for n in names})
    plan_by_name = {p["name"]: p for p in plan}

    # 远程不支持复制: 动手前一次说清, 别让每个文件都报一遍同样的错。
    # 这里绝不调用 copyto —— 123 的 COPY 返回 500, 调用只会白等。
    if remote and mode == "copy":
        msg = _source.COPY_UNSUPPORTED
        log(f"✗ {msg}")
        errors = {n: msg for n in names}
        return ({"ok": 0, "fail": len(names), "skip": 0, "total": len(names),
                 "mode": mode, "dest": dest_dir, "remote": True}, errors)

    ok = fail = skip = 0
    errors = {}
    skipped = {}
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
        if not p.get("targets"):
            # 筛选后没有保留任何值 -> 这个作品不该被处理
            errors[n] = "筛选后没有保留任何值,跳过"
            log(f"  - {n[:70]}: 筛选后无值,跳过")
            if progress:
                progress(i, total)
            continue
        src = p["src"]
        if not src:
            fail += 1
            errors[n] = "库里查不到路径"
            continue
        if remote:
            # 单个文件的网络错误不能中断整批: 移动不可逆, 已搬的必须在
            # 统计里体现出来, 后面的文件也不该被一个抖动带停。
            try:
                if not source.exists(_rel_of(src, source.root)):
                    fail += 1
                    errors[n] = f"源文件不存在: {src}"
                    log(f"  ✗ {n[:70]}: {errors[n]}")
                    if progress:
                        progress(i, total)
                    continue
            except Exception as e:
                fail += 1
                errors[n] = f"检查源文件失败: {e}"
                log(f"  ✗ {n[:70]}: {errors[n]}")
                if progress:
                    progress(i, total)
                continue
        elif not os.path.isfile(src):
            fail += 1
            errors[n] = f"源文件不存在: {src}"
            log(f"  ✗ {n[:70]}: {errors[n]}")
            if progress:
                progress(i, total)
            continue
        targets = p["targets"]
        if p.get("dropped"):
            log(f"  ⚠ {n[:60]}: {len(p['dropped'])} 个值超过单个作品 "
                f"{scanner.VALUE_FANOUT_LIMIT} 个的上限, 未建目录: "
                + ", ".join(p["dropped"][:3])
                + (" …" if len(p["dropped"]) > 3 else ""))
        if p.get("merged"):
            for v, kept in p["merged"]:
                log(f"  ⚠ {n[:60]}: {v!r} 与 {kept!r} 清理后同名, "
                    "会并进同一个目录")
        if remote:
            try:
                success, msg = _remote_transfer(source, src, targets, mode,
                                                log)
            except Exception as e:
                success, msg = False, f"{type(e).__name__}: {e}"
            if success:
                ok += 1
            elif isinstance(msg, str) and msg.startswith("SKIP:"):
                skip += 1
                skipped[n] = msg[5:]
                errors[n] = msg[5:]
                log(f"  - {n[:70]}: {msg[5:]}")
            else:
                fail += 1
                errors[n] = msg
                log(f"  ✗ {n[:70]}: {msg}")
            if progress:
                progress(i, total)
            continue
        # 扇出: 先把除第一个外的目标都复制出来,再搬第一个(移动模式下删源)。
        # 顺序不可颠倒 —— 先移动会把源搬走,后面的副本就没源可读了。
        copy_failed = None
        skipped_n = 0
        for t in targets[1:]:
            try:
                cok, cmsg = _copy_verified(src, t, log)
            except Exception as e:
                cok, cmsg = False, f"{type(e).__name__}: {e}"
            if not cok:
                if cmsg.startswith("SKIP:"):
                    skipped_n += 1
                    continue          # 这个目标已有同名文件, 跳过它
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
            if len(targets) > 1:
                extra = f" (另 {skipped_n} 个目录已存在,跳过)" if skipped_n else ""
                log(f"  ✓ {n[:60]} -> {len(targets)} 个目录{extra}")
        elif isinstance(msg, str) and msg.startswith("SKIP:"):
            # 目标已有同名文件: 跳过。跳过不算失败, 也不删源(移动模式下安全)
            skip += 1
            skipped[n] = msg[5:]
            errors[n] = msg[5:]      # 也放进 errors, 界面统一显示原因
            log(f"  - {n[:70]}: {msg[5:]}")
        else:
            fail += 1
            errors[n] = msg
            log(f"  ✗ {n[:70]}: {msg}")
        if progress:
            progress(i, total)

    msg_tail = f",跳过 {skip}" if skip else ""
    log(f"完成: 成功 {ok},失败 {fail}{msg_tail},目标 {dest_dir}")
    return {"ok": ok, "fail": fail, "skip": skip, "total": total,
            "mode": mode, "dest": dest_dir}, errors

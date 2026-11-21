#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""数据源层: 把「文件在哪、怎么读、怎么搬」抽成一套接口。

上层(扫描 / 分类整理)只关心"有哪些压缩包、每个多大、能不能打开、
搬到哪去了", 不该到处写 `if 远程`。所以这里定义 Source:

    LocalSource   —— 本地磁盘, os / shutil
    远程由 RcloneSource 实现, 走 rclone 子进程

路径约定: 远程写成 `remote:path`(如 `123:/本图/本子zip`), 本地就是
普通绝对路径。`get_source()` 按这个自动分派。

几处实测结论决定了实现方式(2026-10-24, 123 云盘 WebDAV):
  * 列目录是网络往返。单层 0.7 秒, 递归列 3850 个文件要 22 秒,
    所以**一次列全并缓存**, 之后问大小/存在都查缓存。
  * moveto 是服务端移动, 0.65 秒/文件, 不重新上传。
  * **copy/copyto 不支持**: 服务端返回 500 Internal Server Error。
    这里直接拒绝, 绝不调用 —— 调了只会一路重试到超时, 界面像卡死。
  * moveto 会**静默覆盖**已存在的目标。本项目的口径是"目标已存在就
    跳过、不覆盖", 所以移动前必须自己先查一次。
  * 远程 zip 可以用 `rclone cat --offset/--count` 做区间读, 包成
    RemoteFile 就能直接交给 zipfile, 不用下载整个文件。

注意: 本模块不在文件顶层 import scanner / fileops(避免循环依赖),
需要时在函数内部 import。删除本文件不会影响本地功能的编译。
"""

import json
import os
import re
import shutil
import subprocess
import threading
import time
from datetime import datetime

# 单个 rclone 命令的超时。列目录在大库上实测动辄 20 秒以上,
# 单文件操作 0.7 秒; 分开设, 别让大目录整根失败。
RCLONE_LIST_TIMEOUT = 300
RCLONE_OP_TIMEOUT = 60
RCLONE_TIMEOUT = RCLONE_OP_TIMEOUT      # 兼容旧引用

# 远程解析的并发度。实测(123 云盘 WebDAV, 2026-10-24):
#   * 并发 2 跑几十个没问题, 但连续读一大批后服务端开始限流,
#     对一部分请求直接回**空内容**(退出码 0、stderr 为空)
#   * 并发 4 更快触发, 且总吞吐反而下降
# 限流是服务端行为, 不是本地算力问题, 调大只会更糟。
# 所以默认 1(串行): 慢但确定性高。想要快点可以调这里, 代价是
# 更容易撞上限流 —— 撞上了会重试, 重试完仍空则按"服务端限流"报错,
# 不会伪装成"文件损坏"。
REMOTE_WORKERS = 1

# 单次区间读返回空时重试几次。空响应是服务端限流的信号,
# 退一步再问通常就好了。必须显式判断, 不能把空数据当正常内容 ——
# 那会让 zipfile 报"文件损坏", 看起来像文件本身坏了。
READ_RETRIES = 3
READ_RETRY_WAIT = 1.0

# 读远程文件时的预读块大小。zip 的中央目录在文件尾部, 元数据文件本身
# 很小。实测(123 WebDAV)每次请求约 1.4~1.7 秒, 且**与读多少字节基本
# 无关** —— 成本全在网络往返。所以一次多读点更划算: 尾部 1MB 通常
# 同时覆盖 EOCD 和整个中央目录, 把每个文件从 3 次请求降到 2 次。
READAHEAD = 1024 * 1024

# 这些扩展名才当"可解析的漫画压缩包"; 与 scanner 保持一致。
ARCHIVE_EXT = (".zip", ".cbz")
NOTICE_EXT = (".rar", ".7z")


class SourceError(Exception):
    """数据源操作失败(网络、权限、rclone 不存在等)。"""


# 远程不支持复制时的统一文案。123 云盘 WebDAV 的 COPY 实测返回
# 500 Internal Server Error, 调用只会白等, 所以直接告诉用户改移动。
COPY_UNSUPPORTED = ("该远程不支持复制(123 云盘 WebDAV 的 COPY 返回 500), "
                    "请改用移动")


# ------------------------------------------------------------------ 路径识别

def is_remote(path):
    """`123:/xxx` 是远程; `C:/xxx` 这种 Windows 盘符不是。

    判定与 ba-organizer 保持一致, 免得两处口径不同:
    remote 名非空、不含分隔符, 后面紧跟 `/`; 单字母盘符排除。
    """
    if not path or ":" not in path:
        return False
    head, _sep, rest = path.partition(":")
    if not head or "/" in head or "\\" in head:
        return False
    if not rest.startswith("/"):
        return False
    if len(head) == 1 and head.isalpha():
        return False
    return True


def split_remote(path):
    """`123:/a/b` -> ("123", "/a/b")。本地路径返回 (None, path)。"""
    if not is_remote(path):
        return None, path
    head, _sep, rest = path.partition(":")
    return head, rest or "/"


def join(root, rel):
    """拼完整路径。远程用 `/`, 本地用 os.sep。"""
    if is_remote(root):
        base = root.rstrip("/")
        rel = (rel or "").lstrip("/")
        return f"{base}/{rel}" if rel else base
    return os.path.join(root, rel) if rel else root


def _remote_name(path):
    return path.split(":", 1)[0]


def same_store(a, b):
    """两个根是否在同一存储上(本地对本地, 或同一个 rclone remote)。

    跨存储搬运只能下载再上传, 本项目不做 —— 所以除了"同为本地"和
    "同一个 remote", 其余都要在入口处拒绝。
    """
    ra, rb = is_remote(a), is_remote(b)
    if ra != rb:
        return False
    if not ra:
        return True
    return _remote_name(a) == _remote_name(b)


# ------------------------------------------------------------------ 远程文件

class RemoteFile:
    """把 `rclone cat --offset/--count` 包成可 seek 的只读文件对象。

    zipfile 需要 seek 到文件尾部读中央目录, 再按需读每个条目。
    每次都区间读, 不落地整个文件。
    """

    def __init__(self, path, size, run=None):
        self.path = path
        self.size = int(size or 0)
        self.pos = 0
        self.calls = 0
        self._start = 0
        self._buf = b""
        self._run = run or _rclone_cat

    # zipfile 会把这些属性都问一遍
    def seekable(self):
        return True

    def readable(self):
        return True

    def writable(self):
        return False

    def tell(self):
        return self.pos

    def seek(self, offset, whence=0):
        if whence == 0:
            new = offset
        elif whence == 1:
            new = self.pos + offset
        elif whence == 2:
            new = self.size + offset
        else:
            raise ValueError(f"无效 whence: {whence}")
        if new < 0:
            raise ValueError("seek 到负数位置")
        self.pos = new
        return self.pos

    def close(self):
        self._buf = b""

    # scan_one 用 `with src.open_file(rel) as fh:` 统一本地/远程
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    def read(self, n=-1):
        if n is None or n < 0:
            n = self.size - self.pos
        n = min(n, self.size - self.pos)
        if n <= 0:
            return b""
        end = self.pos + n
        # 命中上一次的读缓冲就不发请求
        if self._start <= self.pos and end <= self._start + len(self._buf):
            off = self.pos - self._start
            data = self._buf[off:off + n]
            self.pos = end
            return data
        # 从尾部往前读时一次预读一大块: zipfile 的典型动作是
        # seek(-22, 2) 读 EOCD, 紧接着读中央目录 —— 两者都在尾部。
        # 预读 1MB 让他们命中同一块缓冲区, 请求数减半。
        if self.pos >= self.size - READAHEAD:
            start = max(0, self.size - READAHEAD)
            want = self.size - start
        else:
            start = self.pos
            want = min(max(n, READAHEAD), self.size - self.pos)
        self._buf = self._run(self.path, start, want)
        self.calls += 1
        self._start = start
        off = self.pos - start
        data = self._buf[off:off + n]
        self.pos += len(data)
        return data


def _rclone_cat(path, offset, count):
    """区间读。空响应和**短响应**都要重试。

    123 的 WebDAV 在并发稍高或连续读很多时, 会直接回**空内容**或
    **少于请求长度**的内容(退出码 0、没有报错), 不是慢, 是服务端限流。
    若不判断, zipfile 会拿残缺数据去解析, 报 "File is not a zip file"
    —— 看着像文件坏了, 实际是服务端的问题。
    """
    got = b""
    for attempt in range(READ_RETRIES + 1):
        chunk = _rclone(["cat", path, "--offset", str(offset + len(got)),
                         "--count", str(count - len(got))],
                        timeout=RCLONE_OP_TIMEOUT)
        got += chunk
        if len(got) >= count or count <= 0:
            return got
        if attempt < READ_RETRIES:
            time.sleep(READ_RETRY_WAIT * (attempt + 1))   # 退避, 别越催越糟
    # 重试完还是不够: 这是限流, 不是文件坏了。必须明确说出来,
    # 否则 zipfile 只会报 "File is not a zip file", 把人引向错误的排查方向。
    raise SourceError(
        f"远程读取返回的内容不完整(限流或服务端异常), 已重试 "
        f"{READ_RETRIES} 次: {path.rsplit('/', 1)[-1]} "
        f"@ {offset}+{count} (拿到 {len(got)} 字节)")


# ------------------------------------------------------------------ rclone 调用

# rclone 会把这些环境变量当作全局过滤规则。单文件的 cat/moveto 一旦
# 带着过滤规则, 1.75 会直接报 "can't limit to single files when using
# filters", 连 123 云盘都还没访问就失败了。列目录不需要过滤, 单文件
# 操作更不需要 —— 所以子进程里一律剔除。
_RCLONE_FILTER_ENV = (
    # 直接给命令行追加 --filter / --exclude / --include
    "RCLONE_EXCLUDE", "RCLONE_FILTER", "RCLONE_INCLUDE",
    "RCLONE_FILTER_FROM", "RCLONE_EXCLUDE_FROM", "RCLONE_INCLUDE_FROM",
    # --files-from 系列也会让 rclone 认为"只在清单里操作", 同样不能用于
    # 本项目逐文件的 cat/moveto。
    "RCLONE_FILES_FROM", "RCLONE_FILES_FROM_RAW", "RCLONE_FILES_FROM0",
    # 元数据过滤
    "RCLONE_METADATA_FILTER", "RCLONE_METADATA_EXCLUDE",
    "RCLONE_METADATA_FILTER_FROM", "RCLONE_METADATA_EXCLUDE_FROM",
    # 其它会转化成过滤规则的条件(实测 MAX_SIZE 也触发同样的报错)
    "RCLONE_EXCLUDE_IF_PRESENT", "RCLONE_MAX_SIZE", "RCLONE_MIN_SIZE",
    "RCLONE_MAX_AGE", "RCLONE_MIN_AGE",
)


def _rclone_env():
    """给 rclone 子进程用的环境: 保留配置, 去掉全局过滤规则。"""
    env = dict(os.environ)
    for key in _RCLONE_FILTER_ENV:
        env.pop(key, None)
    return env


def _rclone(args, timeout=RCLONE_OP_TIMEOUT):
    """跑一条 rclone, 返回 stdout(bytes)。失败抛 SourceError。

    参数以列表传入, 不经 shell, 所以没有命令注入面。远程路径总是
    `remote:/...` 形式(以 remote 名开头), 不会被当成选项 —— 这一点
    与拼字符串调 shell 的实现有本质区别。
    """
    cmd = ["rclone"] + list(args)
    try:
        p = subprocess.run(cmd, capture_output=True,
                           timeout=timeout, env=_rclone_env())
    except FileNotFoundError:
        raise SourceError("找不到 rclone, 请先安装并配置好 remote")
    except subprocess.TimeoutExpired:
        raise SourceError(f"rclone 超时({timeout}s): {' '.join(args[:3])}")
    if p.returncode != 0:
        err = (p.stderr or b"").decode("utf-8", "replace").strip()
        last = err.split("\n")[-1] if err else ""
        raise SourceError(f"rclone 失败(退出码 {p.returncode}): {last or '未知错误'}")
    return p.stdout


def _parse_modtime(s):
    """rclone 的 ModTime(RFC3339, 可能带 Z / 偏移) -> epoch 秒。

    取不到就返回 0 —— 调用方把 0 当作"不知道", 会重新解析,
    宁可多读一次也不能漏掉改动。
    """
    if not s:
        return 0
    s = str(s).strip()
    if not s:
        return 0
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return int(dt.timestamp())
    except ValueError:
        pass
    # 兜底: 老格式 "2026-06-11 13:40:24"
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return int(time.mktime(time.strptime(s, fmt)))
        except ValueError:
            continue
    return 0


# ------------------------------------------------------------------ Source 接口

class Source:
    """数据源接口。上层只依赖这些方法。

    返回状态统一为 (status, msg):
      ok / skip / missing / fail / unsupported
    """

    kind = "source"
    supports_copy = True

    def __init__(self, root):
        self.root = root

    # -- 读 ---------------------------------------------------------------
    def list_archives(self, recursive=True):
        """[(rel, name, size, mtime)]。rel 是相对 root 的路径。"""
        raise NotImplementedError

    def exists(self, rel):
        raise NotImplementedError

    def size(self, rel):
        raise NotImplementedError

    def open_file(self, rel):
        """可 seek 的只读文件对象(zipfile 用)。"""
        raise NotImplementedError

    def join(self, rel):
        return join(self.root, rel)

    # -- 写 ---------------------------------------------------------------
    def move(self, src_rel, dst_rel):
        raise NotImplementedError

    def copy(self, src_rel, dst_rel):
        raise NotImplementedError


class LocalSource(Source):
    """本地磁盘。

    接口与 RcloneSource 对齐; 扫描/搬移的本地路径仍然走 scanner /
    fileops 里原有的实现(行为完全不变), 这个类供分类层在显式传入
    数据源时统一使用, 也用于测试本地与远程的口径一致。
    """

    kind = "local"
    supports_copy = True

    def __init__(self, root):
        super().__init__(root.rstrip(os.sep) or os.sep)

    def _abs(self, rel):
        return os.path.join(self.root, rel) if rel else self.root

    def list_archives(self, recursive=True):
        out = []
        exts = ARCHIVE_EXT
        if not os.path.isdir(self.root):
            return out
        if not recursive:
            for fn in sorted(os.listdir(self.root)):
                p = os.path.join(self.root, fn)
                if not os.path.isfile(p):
                    continue
                if os.path.splitext(fn)[1].lower() in exts:
                    st = os.stat(p)
                    out.append((fn, fn, st.st_size, int(st.st_mtime)))
            return out
        for dirpath, dirnames, filenames in os.walk(self.root):
            dirnames.sort()
            for fn in sorted(filenames):
                if os.path.splitext(fn)[1].lower() not in exts:
                    continue
                p = os.path.join(dirpath, fn)
                rel = os.path.relpath(p, self.root)
                try:
                    st = os.stat(p)
                except OSError:
                    continue
                out.append((rel, fn, st.st_size, int(st.st_mtime)))
        return out

    def exists(self, rel):
        return os.path.exists(self._abs(rel))

    def size(self, rel):
        try:
            return os.path.getsize(self._abs(rel))
        except OSError:
            return 0

    def open_file(self, rel):
        return open(self._abs(rel), "rb")

    def move(self, src_rel, dst_rel):
        """移动: 复制 + 校验大小 + 删源(与 fileops 本地口径一致)。"""
        src, dst = self._abs(src_rel), self._abs(dst_rel)
        if os.path.abspath(src) == os.path.abspath(dst):
            return "skip", "源与目标相同"
        if not os.path.exists(src):
            return "missing", "源文件已不存在"
        if os.path.exists(dst):
            return "skip", "目标已存在, 不覆盖"
        try:
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            shutil.copy2(src, dst)
        except OSError as e:
            _silent_remove(dst)
            return "fail", f"复制失败: {e}"
        if not _same_size(src, dst):
            _silent_remove(dst)
            return "fail", "校验失败: 目标大小与源不一致"
        try:
            os.remove(src)
        except OSError as e:
            return "fail", f"已复制但删源失败(内容安全, 源仍在): {e}"
        return "ok", ""

    def copy(self, src_rel, dst_rel):
        src, dst = self._abs(src_rel), self._abs(dst_rel)
        if os.path.abspath(src) == os.path.abspath(dst):
            return "skip", "源与目标相同"
        if not os.path.exists(src):
            return "missing", "源文件已不存在"
        if os.path.exists(dst):
            return "skip", "目标已存在, 不覆盖"
        try:
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            shutil.copy2(src, dst)
        except OSError as e:
            _silent_remove(dst)
            return "fail", f"复制失败: {e}"
        if not _same_size(src, dst):
            _silent_remove(dst)
            return "fail", "校验失败: 目标大小与源不一致"
        return "ok", ""


def _same_size(a, b):
    try:
        return os.path.getsize(a) == os.path.getsize(b)
    except OSError:
        return False


def _silent_remove(path):
    try:
        if os.path.exists(path):
            os.remove(path)
    except OSError:
        pass


class RcloneSource(Source):
    """rclone 远程。

    文件清单一次列全并缓存: 网络往返很贵, 而增量判断、预览、
    搬移都会反复问"这个文件多大/在不在"。
    """

    kind = "rclone"
    # 123 云盘 WebDAV 的 COPY 返回 500。别的 remote 若支持也可以改成 True,
    # 但没验证过就不冒险 —— 换来的只是慢和看起来卡死。
    supports_copy = False

    def __init__(self, root):
        super().__init__(root.rstrip("/") or root)
        # {recursive: {rel: (size, mtime)}} —— 递归与非递归的清单不同,
        # 分开缓存。拿递归清单筛顶层也能用, 但递归列大目录很贵,
        # 不能因为一次非递归调用就把便宜的路径变贵。
        self._files = {}
        self._lock = threading.Lock()

    # -- 内部 -------------------------------------------------------------
    def _root(self):
        return self.root

    def _run(self, args, timeout=RCLONE_OP_TIMEOUT):
        return _rclone(args, timeout=timeout)

    def _ensure(self, recursive=True):
        """取清单。同一层级的清单只拉一次, 之后纯内存查表。"""
        rec = bool(recursive)
        hit = self._files.get(rec)
        if hit is not None:
            return hit
        with self._lock:
            hit = self._files.get(rec)
            if hit is not None:
                return hit
            args = ["lsjson", self.root, "--files-only"]
            if rec:
                args.append("-R")
            out = self._run(args, timeout=RCLONE_LIST_TIMEOUT)
            try:
                items = json.loads(out.decode("utf-8", "replace") or "[]")
            except ValueError as e:
                raise SourceError(f"解析 rclone 输出失败: {e}")
            files = {}
            for it in items:
                if it.get("IsDir"):
                    continue
                rel = it.get("Path") or it.get("Name") or ""
                if not rel:
                    continue
                files[rel] = (int(it.get("Size") or 0),
                              _parse_modtime(it.get("ModTime")))
            self._files[rec] = files
            return files

    def _lookup(self, rel):
        """在已缓存的清单里找 rel; 先查递归清单, 再查顶层清单。

        rel 允许是完整远程路径(`123:/root/a.zip`), 会自动去掉根前缀 ——
        调用方常常手里拿着完整路径, 不该逼它自己算相对路径。
        """
        rel = self._norm(rel)
        for rec in (True, False):
            files = self._files.get(rec)
            if files is not None and rel in files:
                return files[rel]
        # 缓存里没有: 拉顶层(便宜), 还不够再拉递归(贵)
        if rel in self._ensure(False):
            return self._files[False][rel]
        return self._ensure(True).get(rel)

    def _norm(self, rel):
        """完整远程路径 -> 相对本根的路径; 相对路径原样返回。"""
        if not rel:
            return ""
        if is_remote(rel):
            prefix = self.root.rstrip("/") + "/"
            if rel.startswith(prefix):
                return rel[len(prefix):]
            # 不在本根下面: 原样保留, 查不到就是查不到
            return rel
        return rel.lstrip("/")

    def list_archives(self, recursive=True):
        files = self._ensure(recursive)
        out = []
        for rel, (size, mtime) in files.items():
            name = rel.rsplit("/", 1)[-1]
            if os.path.splitext(name)[1].lower() not in ARCHIVE_EXT:
                continue
            if not recursive and "/" in rel:
                continue
            out.append((rel, name, size, mtime))
        out.sort(key=lambda x: x[0])
        return out

    def list_notice(self, recursive=True):
        """rar/7z —— 只统计不解析, 与本地口径一致。"""
        files = self._ensure(recursive)
        out = []
        for rel in files:
            name = rel.rsplit("/", 1)[-1]
            if os.path.splitext(name)[1].lower() not in NOTICE_EXT:
                continue
            if not recursive and "/" in rel:
                continue
            out.append(self.join(rel))
        return out

    def exists(self, rel):
        return self._lookup(rel) is not None

    def size(self, rel):
        hit = self._lookup(rel)
        return hit[0] if hit else 0

    def mtime(self, rel):
        hit = self._lookup(rel)
        return hit[1] if hit else 0

    def open_file(self, rel):
        rel = self._norm(rel)
        hit = self._lookup(rel)
        if hit is None:
            raise SourceError(f"远程文件不存在: {rel}")
        return RemoteFile(self.join(rel), hit[0])

    def stat(self, rel):
        """目标文件的真实状态, 绕过缓存。返回 (exists, size)。

        rel 可以是相对本根源的路径, 也可以是 `123:/...` 完整路径 —
        目标目录通常不在源根下面(分类整理就是搬去别的目录)。
        """
        return self._stat_path(self.join_any(rel))

    def join_any(self, rel):
        """相对路径补上本根; 已经是完整远程路径就原样返回。"""
        if is_remote(rel):
            return rel
        return self.join(rel)

    def _stat_path(self, path):
        try:
            out = self._run(["lsjson", path, "--stat", "--no-mimetype"])
        except SourceError as e:
            msg = str(e)
            # 退出码 3 / directory not found = 文件不存在, 不是错误
            if "退出码 3" in msg or "not found" in msg:
                return False, 0
            raise
        try:
            d = json.loads(out.decode("utf-8", "replace") or "{}")
        except ValueError:
            return False, 0
        if not d or d.get("IsDir"):
            return False, 0
        return True, int(d.get("Size") or 0)

    def ensure_dir(self, path):
        """确保远程目录存在。path 可以是相对本根或完整远程路径。

        本地搬运靠 os.makedirs 建分类目录; 远程 moveto 虽然在某些
        WebDAV 后端会自动补父目录, 但项目需要明确建出「标签/标签值」
        这类目录, 不能依赖服务端隐式行为, 也不能只在日志里显示路径。
        """
        self._run(["mkdir", self.join_any(path)])

    # -- 搬移 -------------------------------------------------------------
    def move(self, src_rel, dst_rel):
        """服务端移动。目标已存在时不覆盖(本项目口径)。

        src_rel / dst_rel 都可以是相对本根的路径, 也可以是完整远程路径
        (分类整理的目标目录通常不在源根下面)。

        rclone moveto 会静默覆盖已有文件, 所以必须先查目标:
          * 不存在      -> 搬
          * 存在且同大小 -> 跳过
          * 存在但不同   -> 失败, 报两个大小, 由用户判断
        """
        src_rel = self._norm(src_rel)
        dst_path = self.join_any(dst_rel)
        # 源是否存在要用**真实 stat**核实, 不能只看缓存: 缓存是整批列目录
        # 时拉的, 用户可能在外部已经把文件搬走了。缓存说"在"就照搬,
        # 结果只会拿到退出码 3, 还被报成"移动失败" —— 语义错了。
        try:
            src_there, want = self._stat_path(self.join(src_rel))
        except SourceError as e:
            return "fail", f"检查源文件失败: {e}"
        if not src_there:
            self._forget(src_rel)          # 顺手把过期的缓存清掉
            return "missing", "源文件已不存在"
        # 缓存说目标在, 也要用真实 stat 复核: 缓存可能过期
        try:
            there, got = self._stat_path(dst_path)
        except SourceError as e:
            return "fail", f"检查目标失败: {e}"
        if there:
            if got == want:
                return "skip", "目标已存在且大小一致"
            return "fail", (f"目标已存在且大小不同({got} != {want}), "
                            "不覆盖, 请手动处理")
        try:
            self._run(["moveto", self.join(src_rel), dst_path,
                       "--retries", "1", "--low-level-retries", "1"])
        except SourceError as e:
            return "fail", str(e)
        # 搬完核对目标: 返回码 0 不等于真的到位(服务端偶发假成功)
        try:
            there, got = self._stat_path(dst_path)
        except SourceError:
            there, got = False, 0
        if not there or got != want:
            return "fail", (f"移动后校验失败: 目标大小 {got} != 源 {want}")
        self._forget(src_rel)
        self._remember(dst_path, want)
        return "ok", ""

    def copy(self, src_rel, dst_rel):
        """远程复制不支持。不调用 subprocess —— 调了会重试到超时。"""
        return "unsupported", COPY_UNSUPPORTED

    def delete(self, rel):
        try:
            self._run(["deletefile", self.join(rel)])
        except SourceError as e:
            raise SourceError(f"删除失败: {e}")
        self._forget(rel)

    def _forget(self, rel):
        key = self._norm(rel)
        for files in self._files.values():
            files.pop(key, None)

    def _rel_key(self, path):
        """完整远程路径 -> 清单的键(相对本根)。

        清单的键一律是相对 root 的 rel。目标常在源根外面(分类整理就是
        搬去别的目录), 那种路径不属于本根的清单, 返回 None —— 别往缓存
        里塞一个永远不会被命中的完整路径键。
        """
        if not is_remote(path):
            return None
        prefix = self.root.rstrip("/") + "/"
        if not path.startswith(prefix):
            return None
        return path[len(prefix):]

    def _remember(self, path, size):
        """把一条已知状态写进缓存(只写属于本根清单的键)。"""
        key = self._rel_key(path)
        if key is None:
            return
        for files in self._files.values():
            files[key] = (size, int(time.time()))

    def invalidate(self):
        with self._lock:
            self._files = {}


# ------------------------------------------------------------------ 工厂

_SOURCES = {}
_SOURCES_LOCK = threading.Lock()


def get_source(root):
    """按根路径取数据源(同根复用, 清单缓存才有意义)。"""
    key = (root or "").rstrip("/") or root
    with _SOURCES_LOCK:
        src = _SOURCES.get(key)
        if src is None:
            src = (RcloneSource(root) if is_remote(root)
                   else LocalSource(root))
            _SOURCES[key] = src
        return src


def drop_source(root=None):
    """丢掉数据源缓存。搬移后目录变了, 想重新列时用。"""
    with _SOURCES_LOCK:
        if root is None:
            _SOURCES.clear()
        else:
            _SOURCES.pop((root or "").rstrip("/") or root, None)

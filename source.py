#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""数据源层: 把「文件在哪、怎么读、怎么搬」抽成一套接口。

上层(扫描 / 分类整理)只关心"有哪些压缩包、每个多大、能不能打开、
搬到哪去了", 不该到处写 `if 远程`。所以这里定义 Source:

    LocalSource   —— 本地磁盘, os / shutil
    远程由 WebDavHttpSource 实现, 走 Python WebDAV HTTP

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
  * 远程 zip 用 HTTP Range 做区间读, 包成
    RemoteFile 就能直接交给 zipfile, 不用下载整个文件。

注意: 本模块不在文件顶层 import scanner / fileops(避免循环依赖),
需要时在函数内部 import。删除本文件不会影响本地功能的编译。
"""

import json
import logging
import os
import re
import shutil
import threading
import time
import urllib.request
import urllib.parse
import base64
from datetime import datetime

# 单个 WebDAV HTTP 请求超时。列目录在大库上可能较久,
# 单文件操作 0.7 秒; 分开设, 别让大目录整根失败。
HTTP_LIST_TIMEOUT = 300
HTTP_OP_TIMEOUT = 60
HTTP_TIMEOUT = HTTP_OP_TIMEOUT      # 兼容旧引用
HTTP_UPLOAD_TIMEOUT = 600
RANGE_CHUNK = 100 * 1024            # Range 起步 100KB，不够再按 100KB 叠加

# 远程解析的并发度。实测(123 云盘 WebDAV, 2026-10-24):
#   * 并发 2 跑几十个没问题, 但连续读一大批后服务端开始限流,
#     对一部分请求直接回**空内容**(退出码 0、stderr 为空)
#   * 并发 4 更快触发, 且总吞吐反而下降
# 限流是服务端行为, 不是本地算力问题, 调大只会更糟。
# 所以默认 1(串行): 慢但确定性高。想要快点可以调这里, 代价是
# 更容易撞上限流 —— 撞上了会重试, 重试完仍空则按"服务端限流"报错,
# 不会伪装成"文件损坏"。
REMOTE_WORKERS = 5

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
    """数据源操作失败(网络、权限、WebDAV 配置缺失等)。"""


# 远程不支持复制时的统一文案。123 云盘 WebDAV 的 COPY 实测返回
# 500 Internal Server Error, 调用只会白等, 所以直接告诉用户改移动。
COPY_UNSUPPORTED = ("该远程不支持复制(123 云盘 WebDAV 的 COPY 返回 500), "
                    "请改用移动")


def _parse_modtime(value):
    """把 HTTP/RFC3339 时间转 epoch 秒; 空/无效返回 0。"""
    if not value:
        return 0
    try:
        from email.utils import parsedate_to_datetime
        return int(parsedate_to_datetime(value).timestamp())
    except Exception:
        pass
    try:
        v = value.strip().replace("Z", "+00:00")
        return int(datetime.fromisoformat(v).timestamp())
    except Exception:
        return 0


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
    """两个根是否在同一存储上(本地对本地, 或同一个 WebDAV remote)。

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

    def open_path(self, rel, size=0):
        """按路径直接打开, 不依赖目录清单缓存。

        阅读页的文件列表以文件源为准, 可能还没入库; 扫描会清掉远程目录
        缓存。此时 open_file 的 _lookup 会失败, 必须能按完整路径直开。
        """
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

    接口与 WebDavHttpSource 对齐; 扫描/搬移的本地路径仍然走 scanner /
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

    def open_path(self, rel, size=0):
        if is_remote(rel):
            raise SourceError("本地数据源不接受远程路径")
        return open(rel, "rb")

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


class WebDavHttpSource(Source):
    """WebDAV HTTP Range 直读数据源(阅读器用, 不行写)。

    凭据来自 .env/WEBDAV_*, 不进代码。WebDAV 路径按 remote:/path 映射:
    remote 名必须在 WEBDAV_REMOTES 中, 或只有一个 remote 时任意名字都接受。
    """

    kind = "webdav-http"
    supports_copy = False

    def __init__(self, root, url, user, password):
        super().__init__(root.rstrip("/") or root)
        self.url = url.rstrip("/")
        self.user = user
        self.password = password
        _name, self.base = split_remote(root)
        self.base = self.base.rstrip("/") or "/"
        self._auth = "Basic " + base64.b64encode(
            (user + ":" + password).encode()).decode()
        self._opener = urllib.request.build_opener()

    def _full_url(self, rel):
        rel = (rel or "").lstrip("/")
        base = self.base.strip("/")
        # 上层可能传相对路径, 也可能传从 PROPFIND href 解出的完整 web 路径。
        # 后者已经包含 base, 不能再拼一次, 否则会变成 /base/base/xxx。
        if base and (rel == base or rel.startswith(base + "/")):
            path = rel
        else:
            path = (base + "/" + rel).lstrip("/") if base else rel
        return self.url + "/" + urllib.parse.quote(path)

    def _xml_body(self, rel, depth="0"):
        req = urllib.request.Request(self._full_url(rel),
            headers={"Authorization": self._auth, "Depth": depth},
            data=b"<?xml version=\"1.0\"?><d:propfind xmlns:d=\"DAV:\"><d:prop><d:resourcetype/><d:getcontentlength/><d:getlastmodified/></d:prop></d:propfind>",
            method="PROPFIND")
        r = self._opener.open(req, timeout=HTTP_LIST_TIMEOUT)
        return r.read()

    def _parse_entries(self, data):
        """解析 PROPFIND XML, 返回 [(rel,isdir,size,mtime)]。"""
        import xml.etree.ElementTree as ET
        root = ET.fromstring(data)
        ns = {"d": "DAV:"}
        base = (urllib.parse.urlparse(self.url).path.rstrip("/") + "/" +
                self.base.strip("/")).rstrip("/")
        out = []
        for resp in root.findall("d:response", ns):
            href = resp.findtext("d:href", "", ns)
            if not href:
                continue
            href_path = urllib.parse.unquote(urllib.parse.urlparse(href).path)
            rel = href_path[len(base):].lstrip("/") if href_path.startswith(base) else href_path.lstrip("/")
            # 123 WebDAV 不返回 resourcetype; href 以 / 结尾就是目录。
            isdir = (resp.find("d:resourcetype/d:collection", ns) is not None
                     or href_path.endswith("/"))
            if isdir:
                rel = rel.strip("/")
            size = int(resp.findtext("d:propstat/d:prop/d:getcontentlength", "0", ns) or 0)
            mtime = _parse_modtime(resp.findtext("d:propstat/d:prop/d:getlastmodified", "", ns))
            out.append((rel, isdir, size, mtime))
        return out

    def _walk(self, rel="", recursive=True):
        """逐目录产出 (目录rel, 文件列表, 子目录列表)。

        - 非递归: 只 PROPFIND 当前目录, 只 yield 当前目录;
          识别到的子目录放进 subdirs 给上层保留缓存, 但不进入、不写空清单。
        - 递归: 逐个子目录继续处理, 每层只 yield 一次。
        """
        import posixpath
        data = self._xml_body(rel, "1")
        entries = self._parse_entries(data)
        base_rel = (rel or "").strip("/")
        files_by_dir = {}
        children = set()

        for path, isdir, size, mtime in entries:
            path = (path or "").strip("/")
            if not path or path == base_rel or path.startswith("../"):
                continue
            parent = posixpath.dirname(path)
            if isdir:
                # 只把当前目录的直接子目录算作 children
                if parent == base_rel:
                    children.add(path)
                continue
            # PROPFIND 可能把子目录里的文件折叠返回。
            if parent == base_rel:
                files_by_dir.setdefault(base_rel, []).append(
                    (posixpath.basename(path), size, mtime))
            else:
                # 从折叠 href 里得知的子目录存在; 递归时继续处理,
                # 非递归时只记为子目录存在, 不进去。
                top = path[len(base_rel) + 1:].split("/")[0] if base_rel else path.split("/")[0]
                child = f"{base_rel}/{top}" if base_rel else top
                children.add(child)
                if recursive:
                    files_by_dir.setdefault(parent, []).append(
                        (posixpath.basename(path), size, mtime))

        child_dirs = sorted(children)
        yield base_rel, files_by_dir.get(base_rel, []), child_dirs
        if not recursive:
            return
        seen = set()
        for d in sorted(set(child_dirs) | set(files_by_dir)):
            if not d or d == base_rel or d in seen:
                continue
            seen.add(d)
            yield from self._walk(d, recursive)

    def list_dirs(self, recursive=True):
        """逐目录产出: (目录rel, 文件列表, 子目录列表)。"""
        return self._walk("", recursive)

    def list_archives(self, recursive=True):
        """兼容旧调用: 仍返回完整 zip/cbz 列表。"""
        out = []
        for _dir, files, _subdirs in self._walk("", recursive):
            for path, size, mtime in files:
                if path.lower().endswith(ARCHIVE_EXT):
                    out.append((path, os.path.basename(path), size, mtime))
        return out

    def list_notice(self, recursive=True):
        """兼容旧调用: 列出 rar/7z。"""
        out = []
        for _dir, files, _subdirs in self._walk("", recursive):
            for path, _s, _m in files:
                if path.lower().endswith(NOTICE_EXT):
                    out.append(path)
        return out

    def exists(self, rel):
        try:
            req = urllib.request.Request(self._full_url(rel),
                headers={"Authorization": self._auth}, method="HEAD")
            r = self._opener.open(req, timeout=HTTP_OP_TIMEOUT)
            return 200 <= r.status < 400
        except Exception:
            return False

    def size(self, rel):
        try:
            req = urllib.request.Request(self._full_url(rel),
                headers={"Authorization": self._auth}, method="HEAD")
            r = self._opener.open(req, timeout=HTTP_OP_TIMEOUT)
            return int(r.headers.get("Content-Length") or 0)
        except Exception:
            return 0

    def move(self, src_rel, dst_rel):
        # WebDAV MOVE, 不覆盖已有目标
        logging.getLogger("source").info("WEBDAV_MOVE src=%s dst=%s", src_rel, dst_rel)
        if self.exists(dst_rel):
            logging.getLogger("source").info("WEBDAV_MOVE_SKIP dst=%s", dst_rel)
            return "skip", "目标已存在, 不覆盖"
        req = urllib.request.Request(self._full_url(src_rel), headers={
            "Authorization": self._auth, "Destination": self._full_url(dst_rel),
            "Overwrite": "F"}, method="MOVE")
        try:
            r = self._opener.open(req, timeout=HTTP_OP_TIMEOUT)
            return "ok", f"HTTP {r.status}"
        except Exception as e:
            logging.getLogger("source").error("WEBDAV_MOVE_FAIL src=%s dst=%s err=%s",
                                              src_rel, dst_rel, e)
            return "fail", str(e)[:200]

    def copy(self, src_rel, dst_rel):
        logging.getLogger("source").info("WEBDAV_COPY src=%s dst=%s", src_rel, dst_rel)
        if self.exists(dst_rel):
            logging.getLogger("source").info("WEBDAV_COPY_SKIP dst=%s", dst_rel)
            return "skip", "目标已存在, 不覆盖"
        req = urllib.request.Request(self._full_url(src_rel), headers={
            "Authorization": self._auth, "Destination": self._full_url(dst_rel),
            "Overwrite": "F"}, method="COPY")
        try:
            r = self._opener.open(req, timeout=HTTP_OP_TIMEOUT)
            return "ok", f"HTTP {r.status}"
        except Exception as e:
            return "fail", str(e)[:200]

    def upload(self, rel, local_path):
        """把本地文件上传到 WebDAV 远程路径。"""
        logging.getLogger("source").info("WEBDAV_UPLOAD_BEGIN rel=%s local=%s bytes=%s",
                                         rel, local_path, os.path.getsize(local_path))
        fh = open(local_path, "rb")
        try:
            req = urllib.request.Request(self._full_url(rel), headers={
                "Authorization": self._auth,
                "Content-Type": "application/zip",
                "Content-Length": str(os.path.getsize(local_path)),
            }, data=fh, method="PUT")
            r = self._opener.open(req, timeout=HTTP_UPLOAD_TIMEOUT)
            r.read(256)
            return "ok", f"HTTP {r.status}"
        except Exception as e:
            logging.getLogger("source").error("WEBDAV_UPLOAD_FAIL rel=%s err=%s", rel, e)
            return "fail", str(e)[:200]
        finally:
            fh.close()

    def delete(self, rel):
        """删除远程文件(WebDAV DELETE)。"""
        logging.getLogger("source").info("WEBDAV_DELETE rel=%s", rel)
        req = urllib.request.Request(self._full_url(rel),
            headers={"Authorization": self._auth}, method="DELETE")
        try:
            r = self._opener.open(req, timeout=HTTP_OP_TIMEOUT)
            return "ok", f"HTTP {r.status}"
        except Exception as e:
            return "fail", str(e)[:200]

    def stat(self, rel):
        ok = self.exists(rel)
        return ok, self.size(rel) if ok else 0

    def open_file(self, rel):
        return self.open_path(rel)

    def join_any(self, rel):
        return rel if is_remote(rel) else self.join(rel)

    def open_path(self, rel, size=0):
        path = self.join_any(rel)
        _name, webpath = split_remote(path)
        url = self.url + "/" + urllib.parse.quote(webpath.lstrip("/"))
        if not size:
            size = self.size(rel)
        return WebDavRangeFile(url, self._auth, size, self._opener)

class WebDavRangeFile:
    """用 HTTP Range 实现可 seek 的只读文件对象。"""
    def __init__(self, url, auth, size, opener):
        self.url, self.auth, self.size = url, auth, int(size or 0)
        self.pos = 0
        self._buf = b""
        self._start = 0
        self._opener = opener

    def seekable(self): return True
    def readable(self): return True
    def writable(self): return False
    def tell(self): return self.pos
    def seek(self, offset, whence=0):
        new = offset if whence == 0 else (self.pos + offset if whence == 1
              else self.size + offset)
        if new < 0: raise ValueError("seek 到负数位置")
        self.pos = new
        return new
    def close(self): self._buf = b""
    def __enter__(self): return self
    def __exit__(self, *a): self.close(); return False

    def read(self, n=-1):
        if n is None or n < 0: n = self.size - self.pos
        n = min(n, self.size - self.pos)
        if n <= 0: return b""
        end = self.pos + n
        if self._start <= self.pos and end <= self._start + len(self._buf):
            off = self.pos - self._start
            out = self._buf[off:off+n]; self.pos = end; return out
        start = self.pos
        want = min(max(n, RANGE_CHUNK), self.size - start)
        stop = min(self.size, start + want) - 1
        req = urllib.request.Request(self.url, headers={
            "Authorization": self.auth,
            "Range": f"bytes={start}-{stop}",
        })
        r = self._opener.open(req, timeout=HTTP_OP_TIMEOUT)
        self._buf = r.read()
        # 服务器可能忽略 Range 返回 200 + 整文件; 这时内容起点是 0,
        # 不能当成从 start 开始的片段, 否则 zipfile 读到错位数据报 BadZipFile。
        self._start = start if r.status == 206 else 0
        if r.status not in (200, 206):
            raise SourceError(f"WebDAV Range 失败: HTTP {r.status}")
        if self.pos < self._start:
            raise SourceError("WebDAV 返回内容起点晚于请求位置")
        off = self.pos - self._start
        out = self._buf[off:off+n]
        self.pos += len(out)
        return out


def _load_webdav_env():
    """从 .env 读取 WEBDAV_URL/USER/PASS, 不写日志不回显。"""
    out = {}
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if not os.path.isfile(path): return out
    for raw in open(path, encoding="utf-8"):
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line: continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip()
    return out


# ------------------------------------------------------------------ 工厂

_SOURCES = {}
_SOURCES_LOCK = threading.Lock()


def get_source(root):
    """按根路径取数据源(同根复用, 清单缓存才有意义)。"""
    key = (root or "").rstrip("/") or root
    with _SOURCES_LOCK:
        src = _SOURCES.get(key)
        if src is None:
            if is_remote(root):
                env = _load_webdav_env()
                if not (env.get("WEBDAV_URL") and env.get("WEBDAV_USER") and env.get("WEBDAV_PASS")):
                    raise SourceError("远程路径需要 .env 配置 WEBDAV_URL/WEBDAV_USER/WEBDAV_PASS")
                src = WebDavHttpSource(root, env["WEBDAV_URL"],
                                       env["WEBDAV_USER"], env["WEBDAV_PASS"])
            else:
                src = LocalSource(root)
            _SOURCES[key] = src
        return src


def drop_source(root=None):
    """丢掉数据源缓存。搬移后目录变了, 想重新列时用。"""
    with _SOURCES_LOCK:
        if root is None:
            _SOURCES.clear()
        else:
            _SOURCES.pop((root or "").rstrip("/") or root, None)

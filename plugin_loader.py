#!/usr/bin/env python3
"""插件加载器: plugins/ 目录下的 .py 自动变成 APP 的一个标签页。

插件约定(往 plugins/ 塞个 .py 重启 APP 即生效,无需编译):

    import flet as ft
    import scanner, fileops          # 可直接用核心库

    NAME = "我的功能"                 # 标签页显示名

    def build(app):
        \"\"\"返回一个 Flet 控件作为页面内容。
        app 参数是 MangaApp 实例,可访问:
          app.page        - Flet Page
          app.current_db() - 当前数据库路径
          app.run_bg(fn)  - 后台线程执行
          app.log(ctrl, msg) - 线程安全写日志
        \"\"\"
        return ft.Column([ft.Text("Hello 插件")])

无 NAME 属性则用文件名。build 抛异常不影响其它插件,
APP 会显示错误信息。
"""

import importlib.util
import os
import sys


def load_plugins(plugins_dir):
    """返回插件模块列表(按文件名排序)。"""
    if not os.path.isdir(plugins_dir):
        return []
    plugs = []
    base = os.path.dirname(os.path.dirname(os.path.abspath(plugins_dir))) \
        or os.path.dirname(plugins_dir)
    for fn in sorted(os.listdir(plugins_dir)):
        if not fn.endswith(".py") or fn.startswith("_"):
            continue
        path = os.path.join(plugins_dir, fn)
        name = "plugin_" + fn[:-3]
        try:
            spec = importlib.util.spec_from_file_location(name, path)
            mod = importlib.util.module_from_spec(spec)
            sys.modules[name] = mod
            spec.loader.exec_module(mod)
            if not hasattr(mod, "build"):
                print(f"[插件] {fn} 缺少 build(app),跳过", file=sys.stderr)
                continue
            if not hasattr(mod, "NAME"):
                mod.NAME = fn[:-3]
            plugs.append(mod)
            print(f"[插件] 已加载 {fn}: {mod.NAME}")
        except Exception as e:
            print(f"[插件] {fn} 加载失败: {e}", file=sys.stderr)
            # 造一个显示错误的占位模块
            import types
            m = types.ModuleType(name)
            m.NAME = fn[:-3] + " (加载失败)"
            def _build(app, _e=e):
                import flet as ft
                return ft.Column([ft.Text(f"插件加载失败: {_e}",
                                          color=ft.Colors.RED)])
            m.build = _build
            plugs.append(m)
    return plugs

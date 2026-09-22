#!/usr/bin/env python3
"""示例插件: 数据库统计一览。

演示插件写法 —— 改这个文件重启 APP 就能更新界面,
或者复制一份改成自己的功能。
"""

import sqlite3

import flet as ft

import scanner

NAME = "统计"


def build(app):
    result_text = ft.Text("点击按钮刷新", selectable=True)

    def refresh(e=None):
        def worker():
            db = app.current_db()
            try:
                conn = sqlite3.connect(db)
                try:
                    total = conn.execute(
                        "SELECT COUNT(*) FROM archives").fetchone()[0]
                    with_meta = conn.execute(
                        "SELECT COUNT(*) FROM archives WHERE has_meta=1"
                        ).fetchone()[0]
                    errors = conn.execute(
                        "SELECT COUNT(*) FROM archives WHERE error IS NOT NULL"
                        ).fetchone()[0]
                    langs = conn.execute(
                        "SELECT m.language, COUNT(*) FROM meta m "
                        "GROUP BY 1 ORDER BY 2 DESC LIMIT 8").fetchall()
                    top_artists = conn.execute(
                        "SELECT m.artist, COUNT(*) FROM meta m "
                        "WHERE m.artist IS NOT NULL "
                        "GROUP BY 1 ORDER BY 2 DESC LIMIT 10").fetchall()
                finally:
                    conn.close()
                lines = [
                    f"压缩包总数: {total}",
                    f"有元数据: {with_meta} / {total}",
                    f"读取失败(待重试): {errors}",
                    "",
                    "语言分布:",
                    *[f"  {l or '未知'}: {c}" for l, c in langs],
                    "",
                    "作者 Top10:",
                    *[f"  {a}: {c}" for a, c in top_artists],
                ]
                app.sync(lambda: _upd("\n".join(lines)))
            except Exception as ex:
                app.sync(lambda: _upd(f"读取失败(先扫描一次?): {ex}"))

        def _upd(text):
            result_text.value = text
            result_text.update()
        app.run_bg(worker)

    btn = ft.FilledButton("刷新统计", icon=ft.Icons.REFRESH, on_click=refresh)
    return ft.Column([
        ft.Text("这是插件示例: 数据库统计", weight=ft.FontWeight.BOLD),
        ft.Text("把新 .py 塞进 plugins/ 重启即新增页面", size=12,
                color="grey600"),
        btn,
        ft.Column([result_text], scroll=ft.ScrollMode.AUTO, expand=True),
    ], spacing=10, expand=True)

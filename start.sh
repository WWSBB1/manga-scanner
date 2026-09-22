#!/bin/sh
# 漫画管理 APP 启动脚本
# 用法: <项目目录>/start.sh   然后浏览器开 http://127.0.0.1:8951
cd <项目目录>
export MANGA_DB="${MANGA_DB:-manga.db}"
export MANGA_PORT="${MANGA_PORT:-8951}"
echo "启动漫画管理 APP..."
echo "数据库: $MANGA_DB"
echo "浏览器打开: http://127.0.0.1:$MANGA_PORT"
echo "局域网: http://$(ip addr show wlan0 2>/dev/null | grep -oP 'inet \K[0-9.]+'):$MANGA_PORT"
echo "Ctrl+C 停止"
echo

# 自动杀掉旧的 app.py 进程,避免 address already in use
old_pids=$(pgrep -f 'python3.*app.py' 2>/dev/null || true)
if [ -n "$old_pids" ]; then
    echo "发现旧进程(PID $old_pids),自动清理..."
    kill $old_pids 2>/dev/null
    sleep 2
fi

python3 -u app.py

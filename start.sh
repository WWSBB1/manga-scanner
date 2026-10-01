#!/bin/sh
# 漫画管理 APP 启动脚本
# 用法: sh start.sh   然后浏览器开 http://127.0.0.1:8951
# 数据库和端口都可以用环境变量覆盖: MANGA_DB / MANGA_PORT

# 定位到本脚本所在目录, 这样 clone 到任何位置都能跑
cd "$(dirname "$(readlink -f "$0")")" || exit 1

export MANGA_DB="${MANGA_DB:-$PWD/manga.db}"
export MANGA_PORT="${MANGA_PORT:-8951}"
echo "启动漫画管理 APP..."
echo "数据库: $MANGA_DB"
echo "浏览器打开: http://127.0.0.1:$MANGA_PORT"

# 局域网地址: 有 ip 用 ip, 没有就退回 ifconfig, 都拿不到就不显示
lan_ip=""
if command -v ip >/dev/null 2>&1; then
    lan_ip=$(ip addr show wlan0 2>/dev/null | grep -o 'inet [0-9.]*' | cut -d' ' -f2)
elif command -v ifconfig >/dev/null 2>&1; then
    lan_ip=$(ifconfig wlan0 2>/dev/null | grep -o 'inet addr:[0-9.]*' | cut -d: -f2)
fi
[ -n "$lan_ip" ] && echo "局域网: http://$lan_ip:$MANGA_PORT"

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

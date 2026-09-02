#!/usr/bin/env bash
set -u

section() {
  echo
  echo "===== $1 ====="
}

section "time and uptime"
date --iso-8601=seconds
uptime
last -x reboot shutdown | head -20

section "memory and swap"
free -h
swapon --show

section "disk space and inodes"
df -h
df -i

section "largest processes"
ps -eo pid,ppid,user,%cpu,%mem,rss,etime,cmd --sort=-rss | head -20

section "kernel OOM evidence: current boot"
journalctl -k -b --no-pager 2>/dev/null | grep -Ei "out of memory|oom-kill|killed process" | tail -80

section "kernel OOM evidence: previous boot"
journalctl -k -b -1 --no-pager 2>/dev/null | grep -Ei "out of memory|oom-kill|killed process" | tail -80

section "PM2 status"
sudo pm2 status

section "PM2 recent errors"
sudo pm2 logs --err --lines 100 --nostream

section "local API"
curl --silent --show-error --max-time 10 http://127.0.0.1:8000/health
echo
curl --silent --show-error --max-time 10 http://127.0.0.1:8000/status
echo

if [[ ${1:-} != "" ]]; then
  section "public domain DNS and HTTPS"
  getent ahosts "$1"
  curl --silent --show-error --location --max-time 15 --write-out "\nHTTP %{http_code} in %{time_total}s; remote=%{remote_ip}\n" "$1/health"
fi

#!/bin/bash
# bg.sh <log> <command...>: run fully detached from the ssh session.
log=$1; shift
cd /root/s11
setsid nohup "$@" < /dev/null > "/root/s11/logs/$log" 2>&1 &
echo "started $! -> logs/$log"

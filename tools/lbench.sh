#!/bin/sh
# Inside the Linux dev container: lbench.sh SRC [N] [extra g++ flags]
SRC=$1; N=${2:-5}; shift 2
g++ -O3 -mcpu=native -std=c++17 -pthread "$@" -o /tmp/pl $SRC || exit 1
cat /data/events/*.csv /data/clicks/*.csv /data/links/*.csv > /dev/null
for i in $(seq $N); do rm -rf /tmp/o; DATA_DIR=/data OUTPUT_DIR=/tmp/o NCPU=8 /tmp/pl 2>&1 | tr '\n' ' '; echo; done

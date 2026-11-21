#!/bin/sh
# bench.sh BIN DATA [N] : run N times, print per-phase min and total min
BIN=$1; DATA=$2; N=${3:-5}
S=${SCRATCH:-/tmp}/bench_out
for i in $(seq $N); do
  rm -rf $S
  /usr/bin/env DATA_DIR=$DATA OUTPUT_DIR=$S NCPU=${NCPU:-8} $BIN 2>&1 | awk '/^\[/{gsub(/[\[\]]/,""); print $2, $1}'
done | awk '{ if(!($1 in m) || $2-prev[$1] < 0) {} ; t[$1]=$2; } { key=$1; v=$2; d=v-last; if (key=="setup") d=v; last=v; if (!(key in mn) || d<mn[key]) mn[key]=d; if (key=="write") { if (!tot || v<tot) tot=v } } END { for (k in mn) printf "%-9s %.3f\n", k, mn[k]; printf "total     %.3f\n", tot }'

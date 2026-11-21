#!/bin/sh
# torture.sh BIN FROM TO
BIN=$1; ROOT=$(cd "$(dirname "$0")/.." && pwd); PY=${PY:-$ROOT/../.venv/bin/python}
W=${SCRATCH:-/tmp}/tort; fail=0
for s in $(seq $2 $3); do
  rm -rf $W; mkdir -p $W
  python3 $ROOT/tools/torture.py $W/d $s
  DATA_DIR=$W/d OUTPUT_DIR=$W/o NCPU=${NCPU:-3} $BIN 2>/dev/null || { echo "seed $s: crash"; fail=1; continue; }
  python3 $ROOT/tools/reference.py $W/d $W/r
  r=$(cd $ROOT/scripts && $PY compare.py $W/o $W/r 2>&1 | tail -1)
  [ "$r" = "MATCH" ] || { echo "seed $s: $r"; fail=1; }
done
[ $fail = 0 ] && echo "all torture seeds match"

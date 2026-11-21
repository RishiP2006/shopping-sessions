#!/bin/sh
# cross.sh BIN EVENTS SEED... : generate, run BIN and reference, compare
BIN=$1; EV=$2; shift 2
ROOT=$(cd "$(dirname "$0")/.." && pwd)
GEN=${GEN:-$ROOT/../ext/gen/generator/target/release/generate}
PY=${PY:-$ROOT/../.venv/bin/python}
W=${SCRATCH:-/tmp}/cross
for seed in "$@"; do
  D=$W/d$seed; rm -rf $D $W/o$seed $W/r$seed
  $GEN --seed $seed --events $EV --clicks $((EV/4)) --output $D >/dev/null || exit 1
  DATA_DIR=$D OUTPUT_DIR=$W/o$seed NCPU=8 $BIN 2>/dev/null || { echo "seed $seed: BIN failed"; continue; }
  python3 $ROOT/tools/reference.py $D $W/r$seed
  printf "seed %s window %s: " $seed "$(grep window_start $D/manifest.json | cut -d'"' -f4)"
  (cd $ROOT/scripts && $PY compare.py $W/o$seed $W/r$seed | tail -1)
  rm -rf $D
done

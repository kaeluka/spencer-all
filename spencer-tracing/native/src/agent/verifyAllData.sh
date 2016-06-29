#!/usr/bin/env bash

source benchRepoLib.sh

function brokenBenchmarks() {
  for BENCH in $(getAllBenchmarks); do
    isCrcCorrect $BENCH || echo $BENCH
  done
}

BROKEN=$(brokenBenchmarks)
for BENCH in $BROKEN; do
  echo " - $BENCH failed due to a broken CRC checksum" 1>&2
done
if [ ! -z "$BROKEN" ]; then
  exit 1
else
  echo "all tracefiles ok"
  exit 0
fi

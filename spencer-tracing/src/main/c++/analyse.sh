#!/usr/bin/env bash
source benchRepoLib.sh

ANALYSER_NAME=$1
ANALYSER=analysers/$ANALYSER_NAME

make $ANALYSER

for BENCH in $(getAllBenchmarks); do
  echo "================="
  echo running $BENCH
  isBenchmarkUpToDate $BENCH && echo "Benchmark is up-to-date" || echo "Benchmark is not up-to-date"
  OUT=$(getBenchdir $BENCH)/$ANALYSER_NAME.out
  if [ -f $OUT ]; then
    CHARS=$(cat $OUT | wc -c | sed "s/\t//g" | sed "s/ //g")
    echo "CHARS=$CHARS"
    if [ "0" != "$CHARS" ]; then
        continue
    fi
  fi
  gzcat $(getPrototracefile $BENCH) | ./$ANALYSER 2>&1 > $(getBenchdir $BENCH)/$ANALYSER_NAME.out
  echo "================="
done

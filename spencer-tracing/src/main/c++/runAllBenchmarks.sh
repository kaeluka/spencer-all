#!/usr/bin/env bash
make || exit 1

PROG=$0

function usage() {
  echo "usage: $PROG [firstBenchmark [small|default|large]]"
  exit 1
}

rm -f *.stdout

SIZE=$2
if [ ! -z "$SIZE" ]; then
  if [ "$SIZE" == "small" -o "$SIZE" == "default" -o "$SIZE" == "large" ]; then
    echo running with size $SIZE
  else
    usage
  fi
fi

STARTED="false"
if [ ! -z $1 ]; then
  echo "==== Running all benchmarks (starting with $1)"
else
  echo "==== Running all benchmarks"
  STARTED="true"
fi

echo "==== Benchmark output goes to <benchname>.stdout"

for B in $(./echoAllBenchs.sh); do
  ./needNewDataForBench.sh $B || continue

  if [ "$B" = "$1" ]; then
    STARTED="true"
  fi
  if [ "$STARTED" = "true"  ]; then
    date
    echo "running benchmark $B"
    ./bench.sh $B $SIZE
    if [ -f $B.stdout ]; then
      DIR=$(cat $B.stdout | grep "left benchmarking data in" | sed "s/left benchmarking data in//")
      echo DIR=$DIR
      if [ ! -z $DIR ]; then
        gzcat $DIR.prototracefile.log.gz | ./count_events
      else
        echo "Something seems to have gone wrong. Check $B.stdout"
      fi
    fi
  fi
done

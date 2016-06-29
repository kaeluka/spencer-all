#!/usr/bin/env bash

echo foo
rm -f test/*_prototrace.log
for t in $(ls test/*.java); do
  TEST=$(echo $t | sed "s/test\///" | sed "s/.java//")
  echo "################# TEST=$TEST"
  ./bench.sh test $TEST > /dev/null || (echo "running test $TEST failed!"; continue)
  analysers/checkTrace "test/"$TEST"_prototrace.log"
  analysers/logTypes "test/"$TEST"_prototrace.log" | head -n100
done

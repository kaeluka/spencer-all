#!/usr/bin/env bash
for BENCH in $(cd tracefiles; find . -type d | grep -v "^\.$"); do
    echo $BENCH
    ./analyseBench.sh $BENCH
done

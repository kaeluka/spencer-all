#!/usr/bin/env bash
for BENCH in $(cd com.github.kaeluka.spencer.tracefiles; find . -type d | grep -v "^\.$"); do
    echo $BENCH
    ./analyseBench.sh $BENCH
done

#! /usr/bin/env bash

## Example: ./needNewDataForBench.sh pmd && ./bench.sh pmd

FAMILY=$1
NEWEST_VERSION=$(./getNewestVersion.sh)

cat com.github.kaeluka.spencer.tracefiles/*$FAMILY*/info.txt | grep $NEWEST_VERSION > /dev/null || exit 0

false

#! /usr/bin/env bash

## Example: ./needNewDataForBench.sh pmd && ./bench.sh pmd

FAMILY=$1
NEWEST_VERSION=$(./getNewestVersion.sh)

cat tracefiles/*$FAMILY*/info.txt | grep $NEWEST_VERSION > /dev/null || exit 0

false

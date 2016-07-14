#!/usr/bin/env bash
source benchRepoLib.sh

BENCH=$1
make -j analyse || exit 1

for ANALYSER_NAME in $(cd analysers; find . -type f -perm +111); do
    if [ $ANALYSER_NAME = "./checkTrace" ]; then
        continue
    fi
    if [ $ANALYSER_NAME = "./traceEvents" ]; then
        continue
    fi
    OUT=$(getBenchdir $BENCH)/$ANALYSER_NAME.out
    CHARS=$(cat $OUT | wc -c | sed "s/\t//g" | sed "s/ //g")
#    echo "CHARS=$CHARS"
    if [ "0" != "$CHARS" ]; then
        echo "already have data from $ANALYSER_NAME. skipping."
        continue
    fi
    gzcat $(getPrototracefile $BENCH) | analysers/$ANALYSER_NAME 2>&1 > $(getBenchdir $BENCH)/$ANALYSER_NAME.out
done

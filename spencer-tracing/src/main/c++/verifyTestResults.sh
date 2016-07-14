#! /usr/bin/env bash

cd test
for v in $(ls ./*.verify.sh); do
    echo $v
    $v
done
cd ..

#! /usr/bin/env bash

source verifyLib.sh

TEST="Lists"

##### Primitives

noneOfNObjsAre $TEST "test/M" "2" "immutable"
noneOfNObjsAre $TEST "test/M" "2" "stationaryObjects"

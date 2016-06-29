#! /usr/bin/env bash

source verifyLib.sh

TESTDIR="Immutable"

 allOfNObjsAre $TESTDIR "test/I" "2" "immutable"
noneOfNObjsAre $TESTDIR "test/M" "2" "immutable"
noneOfNObjsAre $TESTDIR "test/S" "2" "immutable"
 allOfNObjsAre $TESTDIR "test/S" "2" "stationaryObjects"


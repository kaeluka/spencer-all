#! /usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $DIR/versionsLib.sh

#function getBenchmarkDrive() {
#  echo "/Volumes/Elements"
#}

function getFamilies() {
  echo "pmd fop batik eclipse h2 jython luindex lusearch sunflow tomcat xalan avrora" #tradebeans tradesoap
}

function getTracedir() {
  DRIVE=$($DIR/getBenchmarkDrive.sh)
  echo $DRIVE/tracefiles
}

function getBenchdir() {
  if [ -z $1 ]; then
    echo "getBenchdir: need benchmark's name"
    return 1
  fi
  TRACEDIR=$(getTracedir)
  BENCHDIR=$TRACEDIR/$1
  if [ ! -d "$BENCHDIR" ]; then
    echo "getBenchdir: dir ($BENCHDIR) does not exist"
    ls $BENCHDIR
    return 1
  fi
  echo $BENCHDIR
}

# `getLatestFromFamily pmd` will return the latest benchmark directory
# that contains pmd data
function getLatestFromFamily() {
  local FAMILY=$1
  ls $(getTracedir) | grep "^$FAMILY*" | sort -r | head -n1
}

function getLatestFromFamilies() {
  for FAM in $(getFamilies); do
    getLatestFromFamily $FAM
  done
}

function getPrototracefile() {
  BENCHDIR=$(getBenchdir $1) || return 1
  if [ ! -f $BENCHDIR/prototracefile.log.gz ]; then
    echo "getPrototracefile: can not find $BENCHDIR/prototracefile.log.gz"
    exit 1
  fi
  echo $BENCHDIR/prototracefile.log.gz
}

function getInfofile() {
  BENCHDIR=$(getBenchdir $1) || return 1
  if [ ! -f $BENCHDIR/info.txt ]; then
    echo "getPrototracefile: can not find $BENCHDIR/info.txt"
    exit 1
  fi
  echo $BENCHDIR/info.txt
}

function mkEmptyBenchdir() {
  BENCHBASENAME=$1
  if [ -z $BENCHBASENAME ]; then
    echo "mkEmptyBenchdir needs benchmark's base name!"
    exit 1
  fi

  DATESTR=$(date "+%Y-%m-%d_%H-%M")
  TRACEDIR=$(getTracedir)
  BENCHNAME=$BENCHBASENAME"_"$DATESTR
  BENCHDIR=$TRACEDIR/$BENCHNAME
  rm -rf $BENCHDIR
  mkdir $BENCHDIR
  echo $BENCHNAME
}

function doComputeInfoTxt() {
  BENCHDIR=$(getBenchdir $1) || return 1
  DATA=$BENCHDIR/prototracefile.log.gz
  if [ ! -f "$DATA" ]; then
    echo "bench directory ($BENCHDIR) does not contain benchmarking data"
    exit 1
  fi
  INFOTXT=$BENCHDIR/info.txt
  if [ -f $INFOTXT ]; then
    echo "bench directory ($BENCHDIR) already contains info.txt"
    exit 1
  fi
  touch $INFOTXT
  DATACRC=$(crc32 $DATA)
  VERSION=$(getNewestVersion)
  echo "CRC32 = $DATACRC" >> $INFOTXT
  echo "VERSION = $VERSION" >> $INFOTXT

  echo $INFOTXT
}

function isBenchmarkUpToDate() {
  TRACEDIR=$(getTracedir)
  BENCHDIR=$TRACEDIR/$1
  INFOTXT=$BENCHDIR/info.txt
  #cat $INFOTXT
  if [ ! -f $INFOTXT ]; then
    echo "not found: $BENCHDIR/info.txt"
    exit 1
  fi
  SW_VERSION=$(getNewestVersion)
  BENCHMARK_VERSION=$(cat $INFOTXT | grep "VERSION = " | sed "s/VERSION = //")
  if [ -z $BENCHMARK_VERSION ]; then
    BENCHMARK_VERSION="v0.0.0"
  fi
  if [ $SW_VERSION = $BENCHMARK_VERSION ]; then
    return 0
  else
    return 1
  fi
}

function isCrcCorrect() {
  DATA=$(getPrototracefile $1)
  INFO=$(getInfofile $1)
  DATA_CRC=$(crc32 $DATA)
  INFO_CRC=$(cat $INFO | grep "CRC32 = " | sed "s/CRC32 = //")
  if [ "$DATA_CRC" = "$INFO_CRC" ]; then
    true
  else
    false
  fi
}

function getAllBenchmarks() {
  TRACEDIR=$(getTracedir)
  ls $TRACEDIR
}

function doDeleteBench() {
  BENCHDIR=$(getBenchdir $1) || exit 1
  echo "mock deleting $BENCHDIR"
  #rm -rf $BENCHDIR
}

function doGcData() {
  for BENCH in $(getAllBenchmarks); do
    isBenchmarkUpToDate $BENCH &&
      (echo "$BENCH is up to date") ||
      (echo "$BENCH is not up to date"; doDeleteBench $BENCH)
  done
}

#todo: write code that runs new benchmarks and deletes old ones

#doGcData

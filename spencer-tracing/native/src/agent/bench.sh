#!/bin/sh

source benchRepoLib.sh

#CP="-Xbootclasspath/p:.;../transformer/lib/asm-debug-all-5.0.3.jar"
CP="." #"-Xbootclasspath/a:."
PWD=$(pwd)

rm -rf ../transformer/log/*

function run() {
    DACAPO="$PWD/../dacapo/dacapo-9.12-bach.jar"
    if [ ! -e $DACAPO ] ; then
        echo "can't find <$DACAPO>"
        echo "please download from http://www.dacapobench.org/ and rerun this script"
        exit 1
    fi

    TMP=$(mktemp -t protofifo)
    echo "temp fifo is $TMP"
    rm $TMP
    mkfifo $TMP
    gzip < $TMP > $(./getBenchmarkDrive.sh)/prototracefile.log.gz &
    echo "tracing to "$(./getBenchmarkDrive.sh)/prototracefile.log.gz

#    ./restartServer.sh
    java -Xshare:off -Xverify:all -Xfuture -cp $DACAPO:$CP -Xbootclasspath/p:../transformer/instrumented_java_rt/output -agentpath:./lib/NativeInterface.so=tracefile=$TMP $@
#    java -Xverify:all -Xfuture -cp $DACAPO $CP -agentpath:./lib/NativeInterface.so $@
}

BENCH=$1
if [ $BENCH = "test" ] ; then
    echo "running test '$2'"
    echo "logging to test/$2/prototrace.log"

    rm -rf test/$2
    mkdir  test/$2

    #./startServer.sh
    TMP=$(mktemp -t test)
    if [ -z $2 ]; then
      echo "need name of tests. pick one: "
      ls test/*.java | sed "s/.java//" | sed "s/test\// - /"
      exit 1
    fi
    javac test/$2.java

    #./restartServer.sh
    RET="true"
    java -cp .:$CP -Xshare:off -Xbootclasspath/p:../transformer/instrumented_java_rt/output -agentpath:./lib/NativeInterface.so=tracefile=test/$2/prototrace.log test/$2 || RET="false"
    #java -cp .:$CP -Xshare:off -agentpath:./lib/NativeInterface.so=tracefile=test/$2/prototrace.log test/$2 || RET="false"

    cp ../transformer/log/output/test/*.bytecode test/$2/

    for ANALYSER in $(cd analysers; find . -type f -perm +111); do
      echo "analysers is $ANALYSER"
      analysers/$ANALYSER test/$2/prototrace.log > test/$2/$ANALYSER.out
    done
    if [ $RET = "true" ]; then
      exit 0
    else
      exit 1
    fi
else
    SIZE="small"
    if [ ! -z $2 ] ; then
        SIZE=$2
    fi

    echo "running $BENCH with size '$SIZE'"

    RET="passed"
    echo "running benchmark, sending output to $BENCH.stdout..."
    run Harness $1 -s $SIZE --scratch-directory ./scratch/ --preserve --no-validation 2>&1 > $BENCH.stdout || RET="failed"
    #kill $TRANSFORMER_PID; echo "killed"

    if [ "$RET" = "passed" ]; then
      gzip -f $BENCH.stdout
      DRIVE=$(./getBenchmarkDrive.sh)
      if [ -f "$DRIVE/prototracefile.log.gz" ]; then
        BENCHNAME=$(mkEmptyBenchdir "$BENCH-$SIZE")
        BENCHDIR=$(getBenchdir $BENCHNAME)

        mv "$DRIVE/prototracefile.log.gz" $BENCHDIR
        mv $BENCH.stdout.gz $BENCHDIR
        doComputeInfoTxt $BENCHNAME
        echo "left benchmarking data in $BENCHDIR"
        ./verifyTestResults.sh
      fi
    else
      echo "benchmark $BENCH-$SIZE failed. Exiting. Check ./$BENCH.stdout for hints as to why it crashed."
      rm -f /Volumes/Elements/prototracefile.log
      exit 1
    fi
fi

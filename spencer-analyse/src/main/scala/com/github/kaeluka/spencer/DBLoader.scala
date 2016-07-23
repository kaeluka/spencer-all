package com.github.kaeluka.spencer

import java.io.File

import com.github.kaeluka.spencer.Events.AnyEvt
import com.github.kaeluka.spencer.tracefiles.{EventsUtil, SpencerDB, TraceFileIterator}
import com.google.common.base.Stopwatch
import org.apache.spark
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

object DBLoader {

  def main(args: Array[String]) {

    if (args.length != 1) {
      sys.error("usage: java "+this.getClass.getName+" path/to/tracefile")
    }

    val tracefile: File = new File(args(0))
    println("spencer cassandra loader starting...")

    println("checking file " + tracefile)
//    analysis.Util.assertProperCallStructure(new TraceFileIterator(tracefile))
    if (!tracefile.exists) {
      sys.error("file "+tracefile+" does not exist")
    }

    val db = new SpencerDB("test")
    db.loadFrom(tracefile)
    println("done")
    sys.exit(0)
  }
}

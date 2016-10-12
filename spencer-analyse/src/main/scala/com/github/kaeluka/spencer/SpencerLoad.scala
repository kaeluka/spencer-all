package com.github.kaeluka.spencer

import java.io.File

import com.github.kaeluka.spencer.Events.AnyEvt
import com.github.kaeluka.spencer.tracefiles.{EventsUtil, SpencerDB, TraceFileIterator}
import com.google.common.base.Stopwatch
import org.apache.spark
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

object SpencerLoad {

//  val defaultTracefile = "/Volumes/MyBook/tracefiles/pmd.small/tracefile"
//  val defaultTracefile = "/tmp/tracefile"
  val defaultTracefile = "/Users/stebr742/code/kaeluka/spencer-playground/tracefile"


  def main(args: Array[String]) {

    println(args.mkString(", "))

    val tracefile: File =
      if (args.length != 1) {
//        sys.error("usage: java "+this.getClass.getName+" path/to/tracefile")
        System.err.println("no tracefile given (or too many), defaulting to "+defaultTracefile)
        new File(defaultTracefile)
      } else {
        new File(args(0))
    }

    println("spencer cassandra loader starting...")

    if (!tracefile.exists) {
      sys.error("file "+tracefile+" does not exist")
    }

    println("checking file " + tracefile)
//    analysis.Util.assertProperCallStructure(new TraceFileIterator(tracefile))

    val db = new SpencerDB("test")
    db.loadFrom(tracefile)
    println("done")
    sys.exit(0)
  }
}

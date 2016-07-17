package com.github.kaeluka.spencer

import java.io.File

import com.github.kaeluka.spencer.Events.AnyEvt
import com.github.kaeluka.spencer.tracefiles.{EventsUtil, SpencerDB, TraceFileIterator}
import com.google.common.base.Stopwatch
import org.apache.spark
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

object DBLoader {

  def startSpark(): SparkContext = {
    val conf = new SparkConf()
      .setAppName("spencer-analyse")
      .set("spark.cassandra.connection.host", "127.0.0.1")
      .setMaster("local[2]")

    new SparkContext(conf)
  }

  def main(args: Array[String]) {

    if (args.length != 1) {
      sys.error("usage: java "+this.getClass.getName+" path/to/tracefile")
    }

    println("spencer cassandra loader starting...")
    val tracefile: File = new File(args(0))
    if (!tracefile.exists) {
      sys.error("file "+tracefile+" does not exist")
    }

    val db = new SpencerDB("test")
    db.loadFrom(tracefile)
    println("done")
    sys.exit(0)
  }
}

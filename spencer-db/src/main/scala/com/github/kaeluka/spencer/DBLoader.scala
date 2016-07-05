package com.github.kaeluka.spencer

import java.io.File

import com.github.kaeluka.spencer.tracefiles.{EventsUtil, SpencerDB, TraceFileIterator}
import com.google.common.base.Stopwatch
import org.apache.spark
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

object DBLoader {

  def getGraph(sc: SparkContext): Graph[(String, String), String] = {
    val vtx = sc.parallelize(Array(
      (3L, ("rxin", "student")),
      (7L, ("jgonzal", "postdoc")),
      (5L, ("franklin", "prof")),
      (2L, ("istoica", "prof"))))
    val edg = sc.parallelize(Array(
      Edge(3L, 7L, "collab"),
      Edge(5L, 3L, "advisor"),
      Edge(2L, 5L, "colleague"),
      Edge(5L, 7L, "pi")))
    Graph(vtx, edg)
  }

  def startSpark(): SparkContext = {
    val conf = new SparkConf()
      .setAppName("spencer-db")
      .set("spark.cassandra.connection.host", "127.0.0.1")
      .setMaster("local[2]")

    new SparkContext(conf)
  }

  def main(args: Array[String]) {

    println("spencer-db starting...")

    val db = new SpencerDB("test")
    db.loadFrom(new File("/tmp/tracefile"))

    val sc : SparkContext = startSpark()

    new TraceFileIterator(new File("/tmp/tracefile"))
      .take(30)
      .foreach(
        evt => println(EventsUtil.messageToString(evt))
      )

    sc.stop()

    System.exit(0)

//    val started: Stopwatch = Stopwatch.createStarted()
//    val par = sc.parallelize(new TraceFileIterator(new File("/tmp/tracefile")).toSeq)
//    val size = par.count()
//    System.out.println("got "+size+" events in "+started.stop())

  }
}

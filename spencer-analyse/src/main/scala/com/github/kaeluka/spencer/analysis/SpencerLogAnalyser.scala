package com.github.kaeluka.spencer.analysis

import com.github.kaeluka.spencer.Events
import com.github.kaeluka.spencer.tracefiles.{SpencerDB, TraceFileIterator}
import java.io.{File, FileInputStream}

import com.google.common.base.Stopwatch

trait SpencerLogAnalyser[T] extends App {
  def analyse(log: java.util.Iterator[Events.AnyEvt.Reader]) : T
  val defaultPath: String = "/Volumes/MyBook/tracefile"

  override def main(args: Array[String]) {
    val inputFile: File =
      if (args.length != 1) {
//        throw new IllegalArgumentException("usage: java " + this.getClass.getName + " path/to/tracefile")
        System.err.println("no log file path given, using default path: "+defaultPath)
        new File(defaultPath)
      } else {
        new File(args(0))
      }
    val it = new TraceFileIterator(new FileInputStream(inputFile))

    val watch = Stopwatch.createStarted()

    println(this.analyse(it))
    println("log analysis <"+this.getClass.getSimpleName +"> of file "+inputFile+" took "+watch.stop)

    System.exit(0)
  }
}

package com.github.kaeluka.spencer.analysis

import com.github.kaeluka.spencer.Events
import com.github.kaeluka.spencer.tracefiles.{SpencerDB, TraceFileIterator}
import java.io.File

import com.google.common.base.Stopwatch

trait SpencerLogAnalyser[T] extends App {
  def analyse(log: java.util.Iterator[Events.AnyEvt.Reader]) : T;

  override def main(args: Array[String]) {
    if (args.length != 1) {
      throw new IllegalArgumentException("usage: java " + this.getClass.getName + " path/to/tracefile")
    }
    val inputFile: File = new File(args(0))
    val it = new TraceFileIterator(inputFile)

    val watch = Stopwatch.createStarted()

    println(this.analyse(it))
    println("log analysis <"+this.getClass.getSimpleName +"> of file "+inputFile+" took "+watch.stop)

    System.exit(0)
  }
}

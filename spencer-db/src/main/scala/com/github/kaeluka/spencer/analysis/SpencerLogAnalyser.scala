package com.github.kaeluka.spencer.analysis

import com.github.kaeluka.spencer.Events
import com.github.kaeluka.spencer.tracefiles.{SpencerDB, TraceFileIterator}
import java.io.File

trait SpencerLogAnalyser extends App {
  def analyse(log: Iterator[Events.AnyEvt.Reader]);

  override def main(args: Array[String]) {
    val file: File = new File("/tmp/tracefile")
    val it = new TraceFileIterator(file)

    this.analyse(it)

    System.exit(0)
  }
}

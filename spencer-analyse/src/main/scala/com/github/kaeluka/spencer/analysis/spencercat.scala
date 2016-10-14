package com.github.kaeluka.spencer.analysis

import java.io.{File, FileInputStream, InputStream}
import java.util
import java.util.logging.LogManager

import com.github.kaeluka.spencer.Events.AnyEvt
import com.github.kaeluka.spencer.tracefiles.{EventsUtil, TraceFile}
import sun.nio.cs.StreamDecoder

object spencercat extends App {

  override def main(args: Array[String]): Unit = {
    val defaultFile: File = new File("/tmp/tracefile")
    val iter = if (args.length != 1) {
      System.err.println("Warning: no file given, using default file instead: "+defaultFile)
      new TraceFile(new FileInputStream(defaultFile)).iterator
    } else {
      System.out.println("using: "+args(0))
      new TraceFile(new FileInputStream(new File(args(0)))).iterator
    }

    var cnt = 1
    while (iter.hasNext) {
      val evt: AnyEvt.Reader = iter.next
      val string: String = EventsUtil.messageToString(evt)
      println("#" + cnt + ": " + string)
      cnt += 1
    }
  }
}

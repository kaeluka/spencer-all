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
      new TraceFile(new FileInputStream(new File(args(0)))).iterator
    }

    var cnt = 1
    while (iter.hasNext) {
      val evt: AnyEvt.Reader = iter.next
//      if (cnt >= 1355812 && cnt <= 1356115) {
      val string: String = EventsUtil.messageToString(evt)
      if (string.contains("-91972")) {
        println("#" + cnt + ": " + string) //.replace("89173", "<THE OBJECT>"))
      }
//      }
      cnt += 1
      //      if (cnt > 1521795+100) {
      //        sys.exit(0)
      //        LogManager
      //      }
    }
  }
}

package com.github.kaeluka.spencer

import com.github.kaeluka.spencer.tracefiles.{EventsUtil, TraceFiles}

object SpencerCat extends App {

  override def main(args: Array[String]): Unit = {
    val defaultFile = "/tmp/tracefile"
    val iter = if (args.length != 1) {
      System.err.println("Warning: no file given, using default file instead: "+defaultFile)
      TraceFiles.fromPath(defaultFile).iterator
    } else {
      System.out.println("using: "+args(0))
      TraceFiles.fromPath(args(0)).iterator
    }

    var cnt = 1
    while (iter.hasNext) {
      println(s"#$cnt: ${EventsUtil.messageToString(iter.next)}")
      cnt += 1
    }
  }
}

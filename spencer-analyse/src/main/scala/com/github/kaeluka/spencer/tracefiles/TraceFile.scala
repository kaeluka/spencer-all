package com.github.kaeluka.spencer.tracefiles
import java.io.{File, InputStream}
import java.nio.file.Path
import java.util.{Spliterator, Spliterators}
import java.util.stream.StreamSupport

import com.github.kaeluka.spencer.Events.AnyEvt

/**
  * Created by stebr742 on 2016-07-16.
  */
class TraceFile(val file : InputStream) extends java.lang.Iterable[AnyEvt.Reader] {
  override def iterator: java.util.Iterator[AnyEvt.Reader] = {
    new TraceFileIterator(this.file)
  }

  def stream() : java.util.stream.Stream[AnyEvt.Reader] = {
    val spliter: Spliterator[AnyEvt.Reader] = Spliterators.spliteratorUnknownSize(this.iterator, 0)
    StreamSupport.stream(spliter, false)
  }
}

object TraceFile {
    def fromPath(p : Path) : TraceFile = {
    null
  }
}

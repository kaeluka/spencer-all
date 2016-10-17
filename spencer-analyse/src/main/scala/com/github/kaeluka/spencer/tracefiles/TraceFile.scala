package com.github.kaeluka.spencer.tracefiles
import java.io._
import java.nio.file.Path
import java.util.{Spliterator, Spliterators}
import java.util.stream.StreamSupport

import com.github.kaeluka.spencer.Events.AnyEvt
import org.apache.commons.compress.archivers.zip.{ZipArchiveEntry, ZipFile}

/**
  * Created by stebr742 on 2016-07-16.
  */

object TraceFiles {
  def fromPath(path: String) : TraceFile = {
    new TraceFile(TraceFiles.getInputStream(path))
  }

  def getInputStream(path: String) : InputStream = {
    if (!new File(path).exists()) {
      throw new FileNotFoundException("file "+path+" does not exist for loading")
    }
    var ret : InputStream = null
    if (path.endsWith(".zip")) {
      val zip: ZipFile = new ZipFile(path)
      val entries = zip.getEntries()
      while (entries.hasMoreElements && ret == null) {
        val element: ZipArchiveEntry = entries.nextElement()
        println("name="+element.getName)
        if (element.getName == "tracefile" || element.getName.endsWith("/tracefile")) {
          ret = zip.getInputStream(element)
        }
      }
      if (ret == null) {
        throw new IOException("could not find tracefile in archive "+path)
      }
    } else {
      ret = new FileInputStream(new File(path))
    }

    assert(ret != null)
    ret
  }
}

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

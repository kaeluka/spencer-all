package com.github.kaeluka.spencer.tracefiles

import java.io.{File, FileInputStream, IOException}
import java.nio.channels.ReadableByteChannel

import com.github.kaeluka.spencer.Events
import com.github.kaeluka.spencer.Events.AnyEvt
import org.capnproto.Serialize

/**
  * Created by stebr742 on 2016-07-01.
  */
class TraceFileIterator(val channel : ReadableByteChannel,
                        var nxtVal : AnyEvt.Reader) extends java.util.Iterator[AnyEvt.Reader] {

  def this(channel : ReadableByteChannel) {
    this(channel, TraceFileIterator.getNxtOrNull(channel))
  }

  def this(file : File) {
    this(new FileInputStream(file).getChannel)
  }

  def next : AnyEvt.Reader = {
    val ret = this.nxtVal
    this.nxtVal = TraceFileIterator.getNxtOrNull(this.channel)
    ret
  }

  def hasNext() : Boolean = {
    this.nxtVal != null
  }

}

object TraceFileIterator {
  def getNxtOrNull(channel: ReadableByteChannel): Events.AnyEvt.Reader = {
    try {
      Serialize.read(channel).getRoot(Events.AnyEvt.factory)
    } catch {
      case ex: IOException =>
        channel.close()
        null
    }
  }
}
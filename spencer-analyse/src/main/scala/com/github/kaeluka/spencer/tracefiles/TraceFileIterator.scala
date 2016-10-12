package com.github.kaeluka.spencer.tracefiles

import java.io.{File, FileInputStream, IOException, InputStream}
import java.nio.channels.{Channel, Channels, ReadableByteChannel}

import com.github.kaeluka.spencer.Events
import com.github.kaeluka.spencer.Events.AnyEvt
import org.capnproto.Serialize

/**
  * Created by stebr742 on 2016-07-01.
  */
class TraceFileIterator(val channel : ReadableByteChannel) extends java.util.Iterator[AnyEvt.Reader] {

  private var nxtVal : AnyEvt.Reader = TraceFileIterator.getNxtOrNull(channel)

  def this(input : InputStream) {
    this(Channels.newChannel(input))
  }

  def next : AnyEvt.Reader = {
    var ret = this.nxtVal
    this.nxtVal = TraceFileIterator.getNxtOrNull(this.channel)
    ret
  }

  def hasNext: Boolean = {
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
package com.github.kaeluka.spencer.analysis
import java.util

import com.github.kaeluka.spencer.Events.AnyEvt
import com.github.kaeluka.spencer.tracefiles.EventsUtil
import com.google.common.base.Stopwatch

object CountEvents extends
  SpencerLogAnalyser[util.HashMap[String, util.HashMap[AnyEvt.Which, Int]]] {

  override def analyse(log: java.util.Iterator[AnyEvt.Reader]): util.HashMap[String, util.HashMap[AnyEvt.Which, Int]] = {
    var cnt : Long = 0;
    val watch = Stopwatch.createStarted()
    val counts: util.HashMap[String, util.HashMap[AnyEvt.Which, Int]] =
      new util.HashMap()

    while (log.hasNext) {

      val next = log.next
      val thdName = EventsUtil.getThread(next)
      if (!counts.containsKey(thdName)) {
        counts.put(thdName, new util.HashMap())
      }
      val innerCount: util.HashMap[AnyEvt.Which, Int] = counts.get(thdName)
      innerCount.put(next.which(), innerCount.get(next.which())+1)
      cnt += 1
    }

    counts
  }
}

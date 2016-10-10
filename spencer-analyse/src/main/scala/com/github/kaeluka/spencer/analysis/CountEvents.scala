package com.github.kaeluka.spencer.analysis
import java.util

import com.github.kaeluka.spencer.Events.AnyEvt
import com.github.kaeluka.spencer.tracefiles.EventsUtil
import com.google.common.base.Stopwatch

object CountEvents extends
  SpencerLogAnalyser[Unit] {

  override def analyse(log: java.util.Iterator[AnyEvt.Reader]): Unit = {
    var cnt : Long = 0
    val watch = Stopwatch.createStarted()
//    val counts: util.HashMap[String, util.HashMap[AnyEvt.Which, Int]] =
//      new util.HashMap()

    while (log.hasNext) {
      val evt = log.next
//      val thdName = EventsUtil.getThread(evt)
//      if (!counts.containsKey(thdName)) {
//        counts.put(thdName, new util.HashMap())
//      }
//      val innerCount: util.HashMap[AnyEvt.Which, Int] = counts.get(thdName)
//      innerCount.put(evt.which(), innerCount.get(evt.which())+1)
      if (cnt % 1e6 == 0) {
        println("#"+(cnt/1e6).toInt+"e6..")
      }
      cnt += 1
    }
  }
}
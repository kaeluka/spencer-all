package com.github.kaeluka.spencer.analysis
import java.util

import com.github.kaeluka.spencer.Events
import com.github.kaeluka.spencer.Events.AnyEvt
import com.github.kaeluka.spencer.Events.AnyEvt.Which
import com.google.common.base.Stopwatch

object CountEvents extends SpencerLogAnalyser {
  override def analyse(log: Iterator[Events.AnyEvt.Reader]): Unit = {
    var cnt : Long = 0;
    val watch = Stopwatch.createStarted()
    val counts: util.HashMap[Events.AnyEvt.Which, Int] = new util.HashMap[Which, Int]()
    while (log.hasNext) {
      if (cnt % 1000000 == 0) {
        println((cnt/1000000)+"e6..")
      }

      val next = log.next
      counts.put(next.which(), counts.get(next.which())+1)
      cnt += 1
    }
    println("had "+cnt+" events")
    println("counting took "+watch.stop)
    println(counts)
  }
}

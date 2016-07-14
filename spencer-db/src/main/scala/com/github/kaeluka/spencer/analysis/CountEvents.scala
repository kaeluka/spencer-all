package com.github.kaeluka.spencer.analysis
import com.github.kaeluka.spencer.Events
import com.github.kaeluka.spencer.Events.AnyEvt
import com.google.common.base.Stopwatch

object CountEvents extends SpencerLogAnalyser {
  override def analyse(log: Iterator[Events.AnyEvt.Reader]): Unit = {
    var cnt : Long = 0;
    val watch = Stopwatch.createStarted()
    while (log.hasNext) {
      if (cnt % 1000000 == 0) {
        println((cnt/1000000)+"e6..")
      }
      cnt += 1
      log.next
    }
    println("had "+cnt+" events")
    println("counting took "+watch.stop)
  }
}

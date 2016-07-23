package com.github.kaeluka.spencer.analysis

import java.util.EmptyStackException

import com.github.kaeluka.spencer.Events
import com.github.kaeluka.spencer.tracefiles.{EventsUtil, TraceFileIterator}
import org.junit.Assert.fail
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers._

object Util {
  def assertProperCallStructure(it: TraceFileIterator) {
    var cnt : Long = 0
    val callStacks: java.util.HashMap[String, java.util.Stack[IndexedEnter]] = new java.util.HashMap[String, java.util.Stack[IndexedEnter]]
    while (it.hasNext()) {
      val evt: Events.AnyEvt.Reader = it.next
      cnt += 1
      if (EventsUtil.getThread(evt) != "DestroyJavaVM") {
        if (evt.isMethodenter) {
          val methodenter: Events.MethodEnterEvt.Reader = evt.getMethodenter
          val thdName: String = methodenter.getThreadName.toString
          if (!callStacks.containsKey(thdName)) callStacks.put(thdName, new java.util.Stack[IndexedEnter])
          callStacks.get(thdName).push(new IndexedEnter(cnt, methodenter))
        }
        else if (evt.isMethodexit) {
          val methodexit: Events.MethodExitEvt.Reader = evt.getMethodexit
          val thdName: String = methodexit.getThreadName.toString
          assertThat(callStacks, hasKey(thdName))
          try {
            val poppedFrame: IndexedEnter = callStacks.get(thdName).pop
            if (poppedFrame.enter.getName.toString != methodexit.getName.toString) {
              assertThat("popped frame's (#" + cnt
                + ") name must match last entered frame (#" + poppedFrame.idx
                + ") in thread '" + thdName + "'\n" + "last entered frame: "
                + EventsUtil.methodEnterToString(poppedFrame.enter)
                , poppedFrame.enter.getName.toString
                , is(methodexit.getName.toString))
            }
          } catch {
            case _ex: EmptyStackException => {
              fail("method exit can not be paired up: " + EventsUtil.messageToString(evt))
            }
          }
        }
      }
    }
  }
}

class IndexedEnter (val idx: Long, val enter: Events.MethodEnterEvt.Reader) {
}

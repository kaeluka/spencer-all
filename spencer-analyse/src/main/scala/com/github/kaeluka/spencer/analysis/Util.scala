package com.github.kaeluka.spencer.analysis

import java.util

import scala.collection.JavaConversions._
import com.github.kaeluka.spencer.Events
import com.github.kaeluka.spencer.tracefiles.{EventsUtil, TraceFileIterator}
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers._
import org.junit.Assert.fail
import sun.nio.cs.StreamDecoder

object Util {

  def assertProperCallStructure(it: TraceFileIterator) {
    var cnt : Long = 0
    val stacks = new CallStackAbstraction
    while (it.hasNext()) {
      val evt: Events.AnyEvt.Reader = it.next
      cnt += 1
      if (EventsUtil.getThread(evt) != "DestroyJavaVM"
        && !EventsUtil.getThread(evt).startsWith("<")) {
        if (evt.isMethodenter) {
          stacks.push(evt.getMethodenter, cnt)
        } else if (evt.isMethodexit) {
          val methodexit: Events.MethodExitEvt.Reader = evt.getMethodexit
          val popped: Either[IndexedEnter, IndexedEnter] = stacks.pop(methodexit)
          popped match {
            case Left(wrongTop) => fail("popped frame's (#" + cnt
              + ") name must match last entered frame (#" + wrongTop.idx
              + ")\n" + "last entered frame: "
              + EventsUtil.methodEnterToString(wrongTop.enter)+"\npopped frame: "
              + EventsUtil.methodExitToString(methodexit))
            case Right(_) => ()
          }
        }
      }
    }
    println("no issues found in " + cnt + " events")
    println("left stack: ")
    for (key <- stacks.stacks.keySet()) {
      val sz = stacks.stacks.get(key).size()
      if (sz != 0) {
        println(key + " -- " + sz + " elements")
      }
    }
  }

  def emptyCallStackAbstraction: util.HashMap[String, util.Stack[IndexedEnter]] = {
    new java.util.HashMap[String, util.Stack[IndexedEnter]]
  }
}

class IndexedEnter (val idx: Long, val enter: Events.MethodEnterEvt.Reader) {
}

class CallStackAbstraction {

  val stacks = new java.util.HashMap[String, util.Stack[IndexedEnter]]

  def push(methodenter: Events.MethodEnterEvt.Reader, idx: Long): Unit = {
    val thdName: String = methodenter.getThreadName.toString
    if (!stacks.containsKey(thdName)) stacks.put(thdName, new java.util.Stack[IndexedEnter])
    stacks.get(thdName).push(new IndexedEnter(idx, methodenter))
  }

  def pop(methodexit: Events.MethodExitEvt.Reader): Either[IndexedEnter, IndexedEnter] = {
    val thdName: String = methodexit.getThreadName.toString
    assertThat(stacks, hasKey(thdName))
    val poppedFrame: IndexedEnter = stacks.get(thdName).pop
    if (poppedFrame.enter.getName.toString != methodexit.getName.toString) {
      Left(poppedFrame)
    } else {
      Right(poppedFrame)
    }
  }

  def peek(thdName: String): Option[IndexedEnter] = {
    val stack: util.Stack[IndexedEnter] = stacks.get(thdName)
    if (stack != null && stack.peek() != null) {
      Some(stack.peek())
    } else {
      None
    }
  }
}

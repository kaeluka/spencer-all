package com.github.kaeluka.spencer.analysis

import java.util
import java.util.EmptyStackException

import scala.collection.JavaConversions._
import com.github.kaeluka.spencer.Events
import com.github.kaeluka.spencer.Events.AnyEvt
import com.github.kaeluka.spencer.Events.AnyEvt.Reader
import com.github.kaeluka.spencer.tracefiles.{EventsUtil, TraceFileIterator}
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers._
import org.junit.Assert.fail
import sun.nio.cs.StreamDecoder

object Util {

  def isTrickyThreadName(name: String) : Boolean = {
    name == "DestroyJavaVM" || name.startsWith("<")
  }

  def assertProperCallStructure(it: TraceFileIterator) {
    var cnt : Long = 0
    val stacks = new CallStackAbstraction
    while (it.hasNext()) {
      val evt: Reader = it.next
      cnt += 1
      if (cnt % 1e6 == 0) {
        println("#" + (cnt/1e6).toInt + "e6..")
      }
      if (!isTrickyThreadName(EventsUtil.getThread(evt))) {
        if (evt.isMethodenter) {
          stacks.push(evt.getMethodenter, cnt)
          //          debugPrint(cnt, stacks, evt)
          ()
        } else if (evt.isMethodexit) {
          val methodexit = evt.getMethodexit
          try {
            //            debugPrint(cnt, stacks, evt)
            val popped: Either[IndexedEnter, IndexedEnter] = stacks.pop(methodexit)
            popped match {
              case Left(wrongTop) =>
                if (!isTrickyThreadName(methodexit.getThreadName.toString)) {
                  //                    debugPrint(cnt + 1, stacks, it.next)
                  fail("popped frame's (#" + cnt
                    + ") name must match last entered frame (#" + wrongTop.idx
                    + ")\n" + "last entered frame: "
                    + EventsUtil.methodEnterToString(wrongTop.enter) + "\npopped frame: "

                    + EventsUtil.methodExitToString(methodexit))
                }
              case Right(_) => ()
            }
          } catch {
            case _: EmptyStackException =>
              if (!isTrickyThreadName(methodexit.getThreadName.toString)) {
                fail("had no stack frames left when executing methodexit #"+cnt+ ": \n" +
                  EventsUtil.methodExitToString(methodexit))
              }
          }
        }
      }
    }
    println("no issues found in " + cnt + " enter/exit events")
    println("left stack: ")
    for (key <- stacks.stacks.keySet()) {
      val sz = stacks.stacks.get(key).size()
      if (sz != 0) {
        println(key + " -- " + sz + " elements")
        for (elt <- stacks.stacks.get(key)) {
          println(elt)
        }
        println(" === ")
      }
    }
  }

  def debugPrint(cnt: Long, stacks: CallStackAbstraction, evt: AnyEvt.Reader) {
    //    (#1509093) name must match last entered frame (#1509054)
    if (cnt == 1509054) {
      println("========================")
    }
    if (cnt >= 1509054 && (evt.isMethodenter || evt.isMethodexit)) {
      val depth: Int = stacks.stacks.get(EventsUtil.getThread(evt)).size()
      println("#" + cnt + ": (\t"+depth+")" + ("  " * depth) + " " + EventsUtil.messageToString(evt))
    }
    if (cnt == 1509054) {
      println("========================")
    }
  }
}

class IndexedEnter (val idx: Long, val enter: Events.MethodEnterEvt.Reader) {
  override def toString: String = {
    "#"+this.idx+": "+EventsUtil.methodEnterToString(this.enter)
  }
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
    try {
      if (stack != null && stack.peek() != null) {
        Some(stack.peek())
      } else {
        None
      }
    } catch {
      case _: EmptyStackException => None
    }
  }
}

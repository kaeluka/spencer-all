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
    name == "DestroyJavaVM" || name.startsWith("<") || name == "Finalizer"
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

case class IndexedEnter (val idx: Long, val enter: Events.MethodEnterEvt.Reader, var usedVariables: Array[Long]) {
  override def toString: String = {
    "#"+this.idx+": "+EventsUtil.methodEnterToString(this.enter)
  }
}

class CallStackAbstraction {

  val stacks = new java.util.HashMap[String, util.Stack[IndexedEnter]]

  def push(methodenter: Events.MethodEnterEvt.Reader, idx: Long): Unit = {
    val thdName: String = methodenter.getThreadName.toString
    if (!stacks.containsKey(thdName)) stacks.put(thdName, new java.util.Stack[IndexedEnter])
    stacks.get(thdName).push(new IndexedEnter(idx, methodenter, new Array[Long](5)))
  }

  def whenWasVarAsUsed(thdName: String, idx: Int, start: Long): Long = {
    val otop = this.peek(thdName)
    otop match {
      case Some(top) =>
        if (top.usedVariables.length > idx)
          top.usedVariables(idx)
        else
          0
      case None =>
        0
    }
  }
  def markVarAsUsed(thdName: String, idx: Int, start: Long): Unit = {
    val otop = this.peek(thdName)
    otop match {
      case Some(top) =>
        val arr = top.usedVariables
        if (idx >= arr.length) {
          top.usedVariables = new Array[Long](idx*2)
          System.arraycopy (arr, 0, top.usedVariables, 0, arr.length)
        }
        top.usedVariables(idx) = start
      case None =>
        ()
    }
  }

  def markVarAsUnused(thdName: String, idx: Int): Unit = {
    this.markVarAsUsed(thdName, idx, 0)
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

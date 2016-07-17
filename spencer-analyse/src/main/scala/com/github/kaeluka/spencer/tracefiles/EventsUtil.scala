package com.github.kaeluka.spencer.tracefiles

import com.github.kaeluka.spencer.Events
import com.github.kaeluka.spencer.Events._

object EventsUtil {

  def getThread(evt: Events.AnyEvt.Reader): String = {
    evt.which() match {
      case Events.AnyEvt.Which.FIELDLOAD =>
        evt.getFieldload.getThreadName.toString
      case Events.AnyEvt.Which.FIELDSTORE =>
        evt.getFieldstore.getThreadName.toString
      case Events.AnyEvt.Which.METHODENTER =>
        evt.getMethodenter.getThreadName.toString
      case Events.AnyEvt.Which.METHODEXIT=>
        evt.getMethodexit.getThreadName.toString
      case Events.AnyEvt.Which.READMODIFY=>
        evt.getReadmodify.getThreadName.toString
      case Events.AnyEvt.Which.VARLOAD=>
        evt.getVarload.getThreadName.toString
      case Events.AnyEvt.Which.VARSTORE=>
        evt.getVarstore.getThreadName.toString
      case other => ("<unknonwn event kind " + other + ">")
    }
  }

  def messageToString(evt: Events.AnyEvt.Reader): String = {
    evt.which() match {
      case Events.AnyEvt.Which.FIELDLOAD =>
        val fload = evt.getFieldload()
        fieldLoadToString(fload)
      case Events.AnyEvt.Which.FIELDSTORE =>
        val fstore = evt.getFieldstore()
        fieldStoreToString(fstore)
      case Events.AnyEvt.Which.METHODENTER =>
        val mthdEnter = evt.getMethodenter()
        methodEnterToString(mthdEnter)
      case Events.AnyEvt.Which.METHODEXIT=>
        val mexit = evt.getMethodexit
        methodExitToString(mexit)

      //      case Events.AnyEvt.Which.OBJALLOC=> "alloc";
      //      case Events.AnyEvt.Which.OBJFREE=> "free";
      case Events.AnyEvt.Which.READMODIFY=>
        val rm = evt.getReadmodify()
        readModifyToString(rm)
      case Events.AnyEvt.Which.VARLOAD=>
        val vl = evt.getVarload()
        varLoadToString(vl)
      case Events.AnyEvt.Which.VARSTORE=>
        val vs = evt.getVarstore()
        varStoreToString(vs)
      case other => ("<unknonwn event kind " + other + ">")
    }
  }

  def varStoreToString(vs: VarStoreEvt.Reader): String = {
    "varstore -" +
      " caller=" + vs.getCallerclass() + " :: " + vs.getCallermethod() + " @ " + vs.getCallertag() +
      ", value= was " + vs.getOldval() + ", is " + vs.getNewval() +
      ", thread=" + vs.getThreadName()
  }

  def varLoadToString(vl: VarLoadEvt.Reader ): String = {
    "varload -" +
      " caller=" + vl.getCallerclass() + " @ " + vl.getCallertag() +
      ", var " + vl.getVar() + "" +
      ", val=" + vl.getVal() +
      ", thread=" + vl.getThreadName()
  }

  def readModifyToString(rm: ReadModifyEvt.Reader): String = {
    "readmodify -" +
      " callee=" + rm.getCalleeclass() + " @ " + rm.getCalleetag() +
      ", caller=" + rm.getCallerclass() + " @ " + rm.getCallertag() +
      (if (rm.getIsModify()) {
        " writes "
      } else {
        " reads "
      }) + rm.getFname()
  }

  def methodExitToString(mexit: MethodExitEvt.Reader): String = {
    "methodExit -" +
      " name=" + mexit.getName() +
      " thread=" + mexit.getThreadName()
  }

  def methodEnterToString(mthdEnter: MethodEnterEvt.Reader ): String = {
    "methodEnter -" +
      " callee=" + mthdEnter.getCalleeclass() + " @ " + mthdEnter.getCalleetag() +
      ", method=" + mthdEnter.getName() + mthdEnter.getSignature() +
      ", callsite=" + mthdEnter.getCallsitefile + ":" + mthdEnter.getCallsiteline +
      ", thread=" + mthdEnter.getThreadName()
  }

  def fieldStoreToString(fstore: FieldStoreEvt.Reader ): String = {
    "fieldStore -" +
      " caller=" + fstore.getCallerclass() + " :: " + fstore.getCallermethod() + " @ " + fstore.getCallertag() +
      ", field=" + fstore.getHolderclass() + " :: " + fstore.getType() + " " + fstore.getFname() + " @ " + fstore.getHoldertag() +
      ", value= was " + fstore.getOldval() + ", is " + fstore.getNewval() +
      ", thread=" + fstore.getThreadName()
  }

  def fieldLoadToString(fload: FieldLoadEvt.Reader ): String = {
    "fieldLoad -" +
      " caller=" + fload.getCallerclass() + " :: " + fload.getCallermethod() + " @ " + fload.getCallertag() +
      ", field=" + fload.getHolderclass() + " :: " + fload.getType() + " " + fload.getFname() + " @ " + fload.getHoldertag() +
      ", thread=" + fload.getThreadName()
  }

  def getCallee(evt: Events.AnyEvt.Reader): Int = {
    evt.which() match {
      case Events.AnyEvt.Which.FIELDLOAD => ???
      case Events.AnyEvt.Which.FIELDSTORE => ???
      case Events.AnyEvt.Which.METHODENTER => ???
      case Events.AnyEvt.Which.METHODEXIT => ???
      case Events.AnyEvt.Which.READMODIFY => ???
      case Events.AnyEvt.Which.VARLOAD => ???
      case Events.AnyEvt.Which.VARSTORE => ???
      case other => throw new IllegalStateException("can not handle event kind " + evt.which())
    }
  }
}

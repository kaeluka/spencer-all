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
      case Events.AnyEvt.Which.LATEINIT =>
        "<unknown thread>"
      case other => throw new AssertionError("<unknown event kind " + other + ">")
    }
  }

  def messageToString(evt: Events.AnyEvt.Reader): String = {
    evt.which() match {
      case Events.AnyEvt.Which.FIELDLOAD =>
        fieldLoadToString(evt.getFieldload)
      case Events.AnyEvt.Which.FIELDSTORE =>
        fieldStoreToString(evt.getFieldstore)
      case Events.AnyEvt.Which.METHODENTER =>
        methodEnterToString(evt.getMethodenter)
      case Events.AnyEvt.Which.METHODEXIT=>
        methodExitToString(evt.getMethodexit)
      //      case Events.AnyEvt.Which.OBJALLOC=> "alloc";
      //      case Events.AnyEvt.Which.OBJFREE=> "free";
      case Events.AnyEvt.Which.READMODIFY=>
        readModifyToString(evt.getReadmodify)
      case Events.AnyEvt.Which.VARLOAD=>
        varLoadToString(evt.getVarload)
      case Events.AnyEvt.Which.VARSTORE=>
        varStoreToString(evt.getVarstore)
      case Events.AnyEvt.Which.LATEINIT=>
        lateInitToString(evt.getLateinit)
      case other => throw new AssertionError("<unknown event kind " + other + ">")
    }
  }

  def lateInitToString(li: LateInitEvt.Reader): String = {
    var ret = "lateInitialisation -" +
      " callee=" + li.getCalleeclass + " @ " + li.getCalleetag + " [ "
    for (i <- 0 until li.getFields.size()) {
      ret += li.getFields.get(i).getName + " = " + li.getFields.get(i).getVal + " "
    }
    //
    ret += "]"
    ret
  }

  def varStoreToString(vs: VarStoreEvt.Reader): String = {
    "varstore -" +
      " caller=" + vs.getCallerclass + " :: " + vs.getCallermethod + " @ " + vs.getCallertag +
      " var "+vs.getVar +
      " , value was " + vs.getOldval + " , now is " + vs.getNewval +
      " , thread=" + vs.getThreadName
  }

  def varLoadToString(vl: VarLoadEvt.Reader ): String = {
    "varload -" +
      " caller=" + vl.getCallerclass + " @ " + vl.getCallertag +
      " , var " + vl.getVar + "" +
      " , val=" + vl.getVal +
      " , thread=" + vl.getThreadName
  }

  def readModifyToString(rm: ReadModifyEvt.Reader): String = {
    "readmodify -" +
      " callee=" + rm.getCalleeclass + " @ " + rm.getCalleetag +
      " , caller=" + rm.getCallerclass + " @ " + rm.getCallertag +
      (if (rm.getIsModify) {
        " writes "
      } else {
        " reads "
      }) + rm.getFname
  }

  def methodExitToString(mexit: MethodExitEvt.Reader): String = {
    "<== (??? @ " +
      mexit.getCname + ") . " + mexit.getName +
      "(???), thread=" + mexit.getThreadName
  }

  def methodEnterToString(mthdEnter: MethodEnterEvt.Reader ): String = {
    "==> " +
      " (" + mthdEnter.getCalleeclass + " @ " + mthdEnter.getCalleetag +
      ") . " + mthdEnter.getName + mthdEnter.getSignature +
      " , callsite=" + mthdEnter.getCallsitefile + ":" + mthdEnter.getCallsiteline +
      " , thread=" + mthdEnter.getThreadName
  }

  def fieldStoreToString(fstore: FieldStoreEvt.Reader ): String = {
    "fieldStore -" +
      " caller=" + fstore.getCallerclass + " :: " + fstore.getCallermethod + " @ " + fstore.getCallertag +
      " , field=" + fstore.getHolderclass + " :: " + fstore.getType + " " + fstore.getFname + " @ " + fstore.getHoldertag +
      " , value= was " + fstore.getOldval + " , is " + fstore.getNewval +
      " , thread=" + fstore.getThreadName
  }

  def fieldLoadToString(fload: FieldLoadEvt.Reader ): String = {
    "fieldLoad -" +
      " caller=" + fload.getCallerclass + " :: " + fload.getCallermethod + " @ " + fload.getCallertag +
      " , field=" + fload.getHolderclass + " :: " + fload.getType + " " + fload.getFname + " @ " + fload.getHoldertag +
      " , thread=" + fload.getThreadName
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

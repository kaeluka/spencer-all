package com.github.kaeluka.spencer.tracefiles

import com.github.kaeluka.spencer.Events

object EventsUtil {
  def messageToString(evt: Events.AnyEvt.Reader): String = {
    evt.which() match {
      case Events.AnyEvt.Which.FIELDLOAD =>
        val fload = evt.getFieldload()
        "fieldLoad -"+
          " caller="+fload.getCallerclass() + " :: " + fload.getCallermethod() + " @ " + fload.getCallertag()+
          ", field="+fload.getHolderclass() + " :: " + fload.getType()+" "+fload.getFname() + " @ " + fload.getHoldertag()+
          ", thread="+fload.getThreadName()
      case Events.AnyEvt.Which.FIELDSTORE =>
        val fstore = evt.getFieldstore()
        "fieldStore -"+
          " caller="+fstore.getCallerclass() + " :: " + fstore.getCallermethod() + " @ " + fstore.getCallertag()+
          ", field="+fstore.getHolderclass() + " :: " + fstore.getType()+" "+fstore.getFname() + " @ " + fstore.getHoldertag()+
          ", value= was "+fstore.getOldval()+", is "+fstore.getNewval()+
          ", thread="+fstore.getThreadName()
      case Events.AnyEvt.Which.METHODENTER =>
        val mthdEnter = evt.getMethodenter()
        "methodEnter -" +
          " callee=" + mthdEnter.getCalleeclass() + " @ " + mthdEnter.getCalleetag() +
          ", method=" + mthdEnter.getName() + mthdEnter.getSignature() +
          ", callsite=" + mthdEnter.getCallsitefile + ":" + mthdEnter.getCallsiteline +
          ", thread=" + mthdEnter.getThreadName()
      case Events.AnyEvt.Which.METHODEXIT=>
        val mexit = evt.getMethodexit
        "methodExit -"+
          " name="+mexit.getName()+
          " thread="+mexit.getThreadName()

      //      case Events.AnyEvt.Which.OBJALLOC=> "alloc";
      //      case Events.AnyEvt.Which.OBJFREE=> "free";
      case Events.AnyEvt.Which.READMODIFY=>
        val rm = evt.getReadmodify()
        "readmodify -"+
          " callee=" + rm.getCalleeclass() + " @ " + rm.getCalleetag() +
          ", caller="+rm.getCallerclass() + " @ " + rm.getCallertag()+
          (if (rm.getIsModify()) { " writes " } else { " reads " }) + rm.getFname()
      case Events.AnyEvt.Which.VARLOAD=>
        val vl = evt.getVarload()
        "varload -"+
          " caller=" + vl.getCallerclass() + " @ " + vl.getCallertag()+
          ", var "+vl.getVar()+""+
          ", val="+vl.getVal()+
          ", thread="+vl.getThreadName()
      case Events.AnyEvt.Which.VARSTORE=>
        val vs = evt.getVarstore()
        "varstore -"+
          " caller="+vs.getCallerclass() +" :: "+vs.getCallermethod()+ " @ " + vs.getCallertag()+
          ", value= was "+vs.getOldval()+", is "+vs.getNewval()+
          ", thread="+vs.getThreadName()
      case other => ("<unknonwn event kind " + other + ">")
    }
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

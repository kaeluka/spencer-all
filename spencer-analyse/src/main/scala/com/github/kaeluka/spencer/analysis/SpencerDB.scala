package com.github.kaeluka.spencer.analysis

sealed trait AproposEvent
case class AproposUseEvent(caller: Long, callee: Long, start: Long, kind: String, name: String, thread: String, msg: String) extends AproposEvent
case class AproposCallEvent(caller: Long, callee: Long, start: Long, end: Long, name: String, callsite: String, thread: String, msg: String) extends AproposEvent
case class AproposRefEvent(holder: Long, referent: Long, start: Long, end: Option[Long], name: String, kind: String, thread: String, msg: String) extends AproposEvent

case class AproposData(alloc: Option[Map[String, Any]], evts: Seq[AproposEvent], klass : Option[String])

object AproposEvent {
  def startTime(aproposEvent: AproposEvent) : Long = {
    aproposEvent match {
      case e: AproposUseEvent  => e.start
      case e: AproposCallEvent => e.start
      case e: AproposRefEvent  => e.start
    }
  }

  def getThread(aproposEvent: AproposEvent) : String = {
    aproposEvent match {
      case e: AproposUseEvent  => e.thread
      case e: AproposCallEvent => e.thread
      case e: AproposRefEvent  => e.thread
    }
  }

  def toJSON(aproposEvent: AproposEvent) : String = {
    val fields = aproposEvent match {
      case e: AproposUseEvent  => List(
        "type:   'use'",
        "callee: "+e.callee,
        "caller: "+e.caller,
        "start:  "+e.start,
        "kind:   '"+e.kind+"'",
        "name:   '"+e.name+"'",
        "thread: '"+e.thread+"'",
        "msg:    '"+e.msg+"'"
      )
      case e: AproposCallEvent => List(
        "type:     'call'",
        "callee:   "+e.callee,
        "caller:   "+e.caller,
        "start:    "+e.start,
        "end:      "+e.end,
        "name:     '"+e.name+"'",
        "callsite: '"+e.callsite+"'",
        "thread:   '"+e.thread+"'",
        "msg:      '"+e.msg+"'"
      )
      case e: AproposRefEvent  => List(
        Some("type:   'ref'"),
        Some("caller: "+e.holder),
        Some("callee: "+e.referent),
        Some("start:  "+e.start),
        Some("name:   '"+e.name+"'"),
        Some("kind:   '"+e.kind+"'"),
        e.end.map("end:    "+_),
        Some("thread: '"+e.thread+"'"),
        Some("msg:    '"+e.msg+"'")
      ).filter(_.isDefined).map(_.get)
    }
    fields.mkString("{\n", ",\n", "\n}")
  }
}

case class BenchmarkMetaInfo(name: String, objCount: Long, date: String, comment: String)


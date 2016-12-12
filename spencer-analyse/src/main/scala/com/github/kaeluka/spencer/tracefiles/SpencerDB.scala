package com.github.kaeluka.spencer.tracefiles

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Path, Paths}
import java.util.EmptyStackException

import com.datastax.driver.core._
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import com.datastax.spark.connector.{CassandraRow, _}
import com.github.kaeluka.spencer.Events
import com.github.kaeluka.spencer.Events.{AnyEvt, LateInitEvt, ReadModifyEvt}
import com.github.kaeluka.spencer.analysis.{CallStackAbstraction, Util}
import com.google.common.base.Stopwatch
import org.apache.commons.io.FileUtils
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

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

sealed trait AproposEvent
case class AproposUseEvent(caller: VertexId, callee: VertexId, start: Long, kind: String, name: String, thread: String, msg: String) extends AproposEvent
case class AproposCallEvent(caller: VertexId, callee: VertexId, start: Long, end: Long, name: String, callsite: String, thread: String, msg: String) extends AproposEvent
case class AproposRefEvent(holder: VertexId, referent: VertexId, start: Long, end: Option[Long], name: String, kind: String, thread: String, msg: String) extends AproposEvent

case class AproposData(alloc: Option[Map[String, Any]], evts: Seq[AproposEvent], klass : Option[String])

object SpencerDB {
  var cluster: Cluster = _
  var sc : SparkContext = _

  def initCluster() = {
    SpencerDB.cluster =
      Cluster.builder()
        .addContactPoint("127.0.0.1")
        //        .addContactPoint("130.238.10.30")
        .build()
  }

  def startSpark () {
    if (! SpencerDB.isSparkStarted) {
      val conf = new SparkConf()
        .setAppName("spencer-analyse")
        .set("spark.cassandra.connection.host", "127.0.0.1")
        .set("spark.network.timeout", "1000")
          .set("spark.executor.heartbeatInterval", "1000")
        //      .set("spark.cassandra.connection.host", "130.238.10.30")
        //      .setMaster("spark://Stephans-MacBook-Pro.local:7077")
//              .set("spark.executor.memory", "4g").set("worker_max_heap", "1g")
        .setMaster("local[8]")

      SpencerDB.sc = new SparkContext(conf)
      //    SpencerDB.sc.setCheckpointDir("/Volumes/MyBook/checkpoints")
    }
  }

  def isSparkStarted : Boolean = {
    SpencerDB.sc != null
  }

  def shutdown() = {
    this.cluster.close()
    SpencerDB.sc.stop()
  }
}

class SpencerDB(val keyspace: String) {
  def shutdown() = {
    this.session.close()
    SpencerDB.shutdown()
  }

  var insertUseStatement : PreparedStatement = _
  var insertEdgeStatement : PreparedStatement = _
  var insertEdgeOpenStatement : PreparedStatement = _
  var finishEdgeStatement : PreparedStatement = _
  var insertCallStatement : PreparedStatement = _
  var insertObjectStatement : PreparedStatement = _
  var session : Session = _
  val saveOrigin = keyspace.equals("test")

  val stacks: CallStackAbstraction = new CallStackAbstraction()


  def handleEvent(evt: AnyEvt.Reader, idx: Long) {
    try {
      evt.which() match {
        case AnyEvt.Which.FIELDLOAD =>
          val fieldload = evt.getFieldload
          insertUse(fieldload.getCallertag,
            fieldload.getHoldertag,
            fieldload.getCallermethod.toString,
            "fieldload",
            fieldload.getFname.toString,
            idx,
            fieldload.getThreadName.toString,
            EventsUtil.messageToString(evt))
        case AnyEvt.Which.FIELDSTORE =>
          val fstore = evt.getFieldstore
//          assert(fstore.getHoldertag != 0, s"edge caller is 0! ${EventsUtil.fieldStoreToString(fstore)}")
          insertUse(
            caller  = fstore.getCallertag,
            callee  = fstore.getHoldertag,
            method  = fstore.getCallermethod.toString,
            kind    = "fieldstore",
            name    = fstore.getFname.toString,
            idx     = idx,
            thread  = fstore.getThreadName.toString,
            comment = EventsUtil.fieldStoreToString(fstore))
          if (fstore.getNewval != 0) {
            openEdge(
              holder  = fstore.getHoldertag,
              callee  = fstore.getNewval,
              kind    = "field",
              name    = fstore.getFname.toString,
              thread  = fstore.getThreadName.toString,
              start   = idx,
              comment = EventsUtil.fieldStoreToString(fstore))
          }

        case AnyEvt.Which.METHODENTER =>
          val menter = evt.getMethodenter
          stacks.push(menter, idx)

          if (menter.getName.toString == "<init>") {
            insertObject(menter.getCalleetag, menter.getCalleeclass.toString,
              menter.getCallsitefile.toString, menter.getCallsiteline,
              menter.getThreadName.toString,
              EventsUtil.messageToString(evt))
          }
        case AnyEvt.Which.METHODEXIT =>
          val mexit = evt.getMethodexit
          try {
            val (returningObjTag: Long, variables : Array[Long]) = stacks.peek(mexit.getThreadName.toString) match {
              case Some(idxdEnter) => (idxdEnter.enter.getCalleetag, idxdEnter.usedVariables)
              case None => 4 // SPECIAL_VAL_JVM
            }
            stacks.pop(mexit) match {
              case Left(x) => println("WARN: no matching call for "+x.toString+"! Was it transformed while the method was running?")// new AssertionError(x.toString)
              case Right(menter) =>
                val callerTag: Long = stacks.peek(mexit.getThreadName.toString) match {
                  case Some(t) => t.enter.getCalleetag
                  case None => 4 // SPECIAL_VAL_JVM
                }
                insertCall(
                  caller       = callerTag,
                  callee       = returningObjTag,
                  name         = menter.enter.getName.toString,
                  start        = menter.idx,
                  end          = idx,
                  thread       = menter.enter.getThreadName.toString,
                  callSiteFile = menter.enter.getCallsitefile.toString,
                  callSiteLine = menter.enter.getCallsiteline,
                  comment      = "")
                var i = 0
                var Nvars = variables.length
                while (i < Nvars) {
                  if (variables(i) > 0) {
                    assert(returningObjTag != 0, s"returningObj can't be 0! ${EventsUtil.methodExitToString(mexit)}")
                    try {
                      closeEdge(
                        holder  = returningObjTag,
                        kind    = "var",
                        start   = variables(i),
                        end     = idx)
                    } catch {
                      case e: AssertionError =>
                        println("method enter was: "+menter)
                        println(s"method exit was:  #$idx: ${EventsUtil.methodExitToString(mexit)}")
                        throw e
                    }
                  }
                  i+=1
                }
            }
          } catch {
            case _: EmptyStackException =>
              throw new AssertionError("#"+idx+": empty stack for " + EventsUtil.messageToString(evt))
          }
        case AnyEvt.Which.LATEINIT =>
          val lateinit: LateInitEvt.Reader = evt.getLateinit
          insertObject(lateinit.getCalleetag
            , lateinit.getCalleeclass.toString
            , "<jvm internals>"
            , -1, "late initialisation")
          for (fld <- lateinit.getFields) {
            if (fld.getVal != 0) {
              openEdge(lateinit.getCalleetag, fld.getVal, "field", fld.getName.toString, "<JVM thread>", 1, "")
            }
          }
        case AnyEvt.Which.READMODIFY =>

          val readmodify: ReadModifyEvt.Reader = evt.getReadmodify
          val caller: Long = readmodify.getCallertag
          val callee: Long = readmodify.getCalleetag
          val kind: String = if (readmodify.getIsModify) {
            "modify"
          } else {
            "read"
          }
          insertUse(
            caller = caller,
            callee = callee,
            method = stacks.peek(readmodify.getThreadName.toString).map(_.enter.getName.toString).getOrElse("<unknown>"),
            kind = kind,
            name = readmodify.getFname.toString,
            idx = idx,
            thread = readmodify.getThreadName.toString,
            comment = if (saveOrigin) EventsUtil.messageToString(evt) else "")
        case AnyEvt.Which.VARLOAD =>
          val varload: Events.VarLoadEvt.Reader = evt.getVarload
          insertUse(
            caller = varload.getCallertag,
            callee = varload.getCallertag,
            method = varload.getCallermethod.toString,
            kind = "varload",
            name ="var_"+varload.getVar.toString,
            idx = idx,
            thread = varload.getThreadName.toString,
            comment = if (saveOrigin) EventsUtil.messageToString(evt) else "")
        case AnyEvt.Which.VARSTORE =>
          val varstore: Events.VarStoreEvt.Reader = evt.getVarstore
          // step 1: emit use:
          insertUse(
            caller = varstore.getCallertag,
            callee = varstore.getCallertag,
            method = varstore.getCallermethod.toString,
            kind = "varstore",
            name ="var_"+varstore.getVar.toString,
            idx = idx,
            thread = varstore.getThreadName.toString,
            comment = if (saveOrigin) EventsUtil.messageToString(evt) else "")
          if (! stacks.peek(varstore.getThreadName.toString).map(_.enter.getCalleetag).contains(varstore.getCallertag) ) {
            println(s"at $idx: ${stacks.peek(varstore.getThreadName.toString)}: varstore is ${EventsUtil.varStoreToString(varstore)}")
          }

          //step 2: set close old reference (if there was one):
          val whenUsed = stacks.whenWasVarAsUsed(varstore.getThreadName.toString, varstore.getVar, idx)
          if (whenUsed > 0) {
            assert(varstore.getCallertag != 0, s"caller must not be 0: ${EventsUtil.varStoreToString(varstore)}")
            closeEdge(
              holder = varstore.getCallertag,
              kind = "var",
              start = whenUsed,
              end = idx)
          }

          //step 3: open new reference (if there is one):
          if (varstore.getNewval != 0) {
            stacks.markVarAsUsed(varstore.getThreadName.toString, varstore.getVar, idx)
            openEdge(
              holder = varstore.getCallertag,
              callee = varstore.getNewval,
              kind = "var",
              name = "var_" + varstore.getVar,
              thread = varstore.getThreadName.toString,
              start = idx,
              comment = EventsUtil.messageToString(evt))
          } else {
            stacks.markVarAsUnused(varstore.getThreadName.toString, varstore.getVar)
          }
        case other =>
          throw new IllegalStateException(
            "do not know how to handle event kind " + other)
      }
    } catch {
      case e: IllegalStateException =>
        val exception: IllegalStateException = new IllegalStateException("cause was: "+EventsUtil.messageToString(evt), e)
    }
  }

  def insertCall(caller: Long, callee: Long, name: String, start : Long, end: Long, thread: String, callSiteFile: String, callSiteLine: Long, comment: String = "none") {
    session.executeAsync(this.insertCallStatement.bind(
      caller : java.lang.Long, callee : java.lang.Long,
      name,
      start : java.lang.Long, end : java.lang.Long,
      thread,
      callSiteFile, callSiteLine : java.lang.Long,
      comment))
  }

  def insertObject(tag: Long, klass: String, allocationsitefile: String, allocationsiteline: Long, thread: String, comment: String = "none") {
    assert(allocationsitefile != null)
    assert(thread != null)
    assert(tag != 0)
    session.executeAsync(this.insertObjectStatement.bind(
      tag : java.lang.Long,
      klass.replace('/','.'),
      allocationsitefile,
      allocationsiteline : java.lang.Long,
      thread,
      if (saveOrigin) comment else ""))
  }

  def insertEdge(caller: Long, callee: Long, kind: String, name: String, thread: String, start: Long, end: Long, comment: String = "none") {
    assert(caller != 0, "caller can't be 0")
    assert(callee != 0, "callee can't be 0")
    assert(kind != null && kind.equals("var") || kind.equals("field"), "kind must be 'var' or 'field'")
    session.executeAsync(this.insertEdgeStatement.bind(
      caller : java.lang.Long,
      callee : java.lang.Long,
      kind,
      name,
      thread,
      start  : java.lang.Long,
      end    : java.lang.Long,
      if (saveOrigin) comment else ""))
  }

  def openEdge(holder: Long, callee: Long, kind: String, name: String, thread: String, start: Long, comment: String = "none") {
    assert(callee != 0, s"callee must not be 0: $comment")
    assert(holder != 0, s"holder must not be 0: $comment")
    assert(kind != null && kind.equals("var") || kind.equals("field"), "kind must be 'var' or 'field'")
    session.executeAsync(this.insertEdgeOpenStatement.bind(
      holder : java.lang.Long,
      callee : java.lang.Long,
      kind,
      name,
      thread,
      start  : java.lang.Long,
      if (saveOrigin) comment else ""))
  }

  def closeEdge(holder: Long, kind: String, start: Long, end: Long) {
    assert(holder != 0, "must have non-zero caller")
    assert(kind != null   && kind.equals("var") || kind.equals("field"), s"kind must be 'var' or 'field', but is $kind")
//    assert(getTable("refs").where("kind = ? AND start = ? AND caller = ?", kind, start, holder).count == 1, s"trying to close edge from $start -- $end: must have exactly one in refs: record with (kind, start, holder) = ${(kind, start, holder)}")
    session.executeAsync(this.finishEdgeStatement.bind(end : java.lang.Long, kind, start : java.lang.Long, holder : java.lang.Long))
  }

  def insertUse(caller: Long, callee: Long, method: String, kind: String, name: String, idx: Long, thread: String, comment: String = "none") {
    assert(caller != 0, s"caller must not be 0: $comment")
    assert(callee != 0, s"callee must not be 0: $comment")
    assert(method != null && method.length > 0, "method name must be given")
    assert(kind != null   && kind.equals("fieldstore") || kind.equals("fieldload") ||kind.equals("varload") ||  kind.equals("varstore") || kind.equals("read") || kind.equals("modify"), s"kind must be 'varstore/load' or 'fieldstore/load' or 'read' or 'modify', but is $kind")
    assert(idx > 0)
    assert(thread != null && thread.length > 0, "thread name must be given")

    session.executeAsync(this.insertUseStatement.bind(
      caller : java.lang.Long,
      callee : java.lang.Long,
      method,
      kind,
      name,
      idx : java.lang.Long,
      thread,
      if (saveOrigin) comment else ""))
  }


  def aproposObject(tag: Long): AproposData = {
    val objTable: CassandraTableScanRDD[CassandraRow] = getTable("objects")

    val theObj = objTable
      .filter(_.getLong("id") == tag)

    val klass = theObj
      .first()
      .getStringOption("klass")

    var allocMsg  = ""
    if (theObj.count == 0) {
      allocMsg += "<not initialised>\n"
    } else {
      allocMsg += theObj.collect()(0)+"\n"
      allocMsg += "allocated at:\n  - "
      allocMsg += theObj
        .map(row => row.getStringOption("allocationsitefile").toString ++":"++row.getLongOption("allocationsiteline").toString)
        .collect.mkString("\n  - ")
      allocMsg += "\n"
    }

    val usesTable: CassandraTableScanRDD[CassandraRow] = this.getTable("uses")
    val uses =
      (usesTable.where("caller = ?", tag) ++
        usesTable.where("callee = ?", tag))
        .map(row=>
          (row.getLong("caller"),
            row.getLong("callee"),
            row.getLong("idx"),
            row.getStringOption("kind").getOrElse("<unknown kind>"),
            row.getStringOption("name").getOrElse("<unknown name>"),
            row.getStringOption("thread").getOrElse("<unknown thread>"),
            row.getString("comment"))
        )

    val useEvents = uses.map {
      case ((caller, callee, idx, kind, name, thread, comment)) => AproposUseEvent(caller, callee, idx, kind, name, thread, "use "+comment).asInstanceOf[AproposEvent]
    }

    val callsTable: CassandraTableScanRDD[CassandraRow] = this.getTable("calls")
    val callsEvents =
      (callsTable.where("caller = ?", tag) ++ callsTable.where("callee = ?", tag))
        .map(row =>
          AproposCallEvent(row.getLong("caller")
            , row.getLong("callee")
            , row.getLong("start")
            , row.getLong("end")
            , row.getString("name")
            , row.getString("callsitefile") + ":" + row.getLong("callsiteline")
            , row.getStringOption("thread").getOrElse("<unknown thread>")
            , row.getString("comment")).asInstanceOf[AproposEvent]
        )

    val refsTable = this.getTable("refs")
    val refsEvents =
      (refsTable.where("caller = ?", tag) ++
        refsTable.where("callee = ?", tag)).map(row =>
        AproposRefEvent(
          row.getLong("caller")
          , row.getLong("callee")
          , row.getLong("start")
          , row.getLongOption("end")
          , row.getString("name")
          , row.getString("kind")
          , row.getStringOption("thread").getOrElse("<unknown thread>")
          , row.getString("comment")).asInstanceOf[AproposEvent])

    val alloc = if (theObj.count > 0) {
      Some(theObj.collect()(0).toMap)
    } else {
      None
    }
    AproposData(
      alloc,
      (useEvents++callsEvents++refsEvents)
        .sortBy(AproposEvent.startTime).distinct().collect(), klass)
  }

  def getTable(table: String): CassandraTableScanRDD[CassandraRow] = {
    assert(SpencerDB.sc != null, "need to have spark context")
    assert(this.session != null, "need to have db session")
    SpencerDB.sc.cassandraTable(this.session.getLoggedKeyspace, table)
  }

  def getProperObjects: RDD[CassandraRow] = {
    //FIXME: use WHERE clause
    getTable("objects")
      .filter(_.getLong("id") > 0)
      .filter(
        _.getStringOption("comment")
          .forall(!_.contains("late initialisation")))
  }

  /**
    * The instrumentation is limited in what byte code it can generate, due to
    * semantics of Java bytecode. This limits how constructor calls can be
    * instrumented. Specifically, constructor and super-constructor calls will
    * appear sequentially (with the innermost constructor first) in the data,
    * not nested as they should be.
    * This method will fix the order to be properly nested for each object.
    *
    * The constructor calls for object 12 of class C (which is a subclass of B,
    * which is a subclass of A) could look like this:
    *
    * #10 call(<init>,12) // A constructor
    * #20 exit(<init>)    // A constructor
    * #30 call(<init>,12) // B constructor
    * #40 exit(<init>)    // B constructor
    * #50 call(<init>,12) // C constructor
    * #60 exit(<init>)    // C constructor
    *
    * Then this method will simply update the start and end times of these calls
    * to be:
    *
    * #10 call(<init>,12) // C constructor
    * #20 call(<init>,12) // B constructor
    * #30 call(<init>,12) // A constructor
    * #40 exit(<init>)    // A constructor
    * #50 exit(<init>)    // B constructor
    * #60 exit(<init>)    // C constructor
    */
  def sortConstructorCalls(): Unit = {
    val correctionMap = this.getTable("calls")
      .filter(call => call.getString("name").equals("<init>"))
      .groupBy(_.getLong("callee"))
      .filter(_._2.size >= 2)
      .map {
        case (callee, _calls) => {
          val calls = _calls.toArray
          val times = calls.flatMap(call => List(
            call.getLong("start"),
            call.getLong("end"))).sorted
          calls.sortBy(call => -1 * call.getLong("start"))
          (callee, calls.zipWithIndex.map {
            case (call, idx) =>
              (
                call,
                (call.getLong("start"), call.getLong("end")) -> (times(idx), times(times.length - idx - 1)))
          })
        }
      }.sortBy(x => x._1)
      .collect()

    correctionMap.foreach {
      case (callee, corrections) =>
        println(s"correcting $callee")
        corrections.foreach {
          case (call, ((origStart, origEnd), (newStart, newEnd))) =>
            session.execute(s"DELETE FROM ${session.getLoggedKeyspace}.calls " +
              s"WHERE caller = ${call.getLong("caller")} AND callee = $callee AND start = $origStart AND end = $origEnd ;")
            insertCall(
              //FIXME: the caller must be THIS, except for the first call
              caller = call.getLong("caller"),
              callee = callee,
              name   = "<init>",
              start  = newStart,
              end    = newEnd,
              thread = call.getString("thread"),
              callSiteFile = call.getString("callsitefile"),
              callSiteLine = call.getLong("callsiteline"),
              comment = call.getString("comment")
            )
        }
    }
  }

  def computeEdgeEnds(): Unit = {
    val groupedAssignments = this.getTable("refs")
      .where("kind = 'field'")
      .select("caller", "name", "start", "kind")
      .groupBy(row => (row.getLong("caller"), row.getString("name")))
      .map({case ((caller, name), rows) =>
        val sorted =
          rows.toSeq.sortBy(_.getLong("start"))
            .map(row =>
              (row.getLong("start"), row.getString("kind"))
            )
        (caller, sorted)
      })
      .collect()

    var cnt = 0
    for ((caller, assignments) <- groupedAssignments) {
      for (i <- 0 to (assignments.size-2)) {
        (assignments(i), assignments(i+1)) match {
          case ((start, kind), (end, _)) =>
            closeEdge(caller, kind, start, end)
            cnt = cnt + 1
        }
      }
    }
    println(cnt + " assignments")
  }

  def computeLastObjUsages(): Unit = {
    assert(SpencerDB.isSparkStarted, "spark must be started in computeLastObjUsages")

    //the first action involving an object is always its constructor call
    val firstLastCalls = this.getTable("calls")
      .select("callee", "start", "end")
      .groupBy(_.getLong("callee"))
      .map {
        case (id, calls) =>
          (id, (calls.map(_.getLong("start")).min, calls.map(_.getLong("end")).max))
      }


    //the last action involving an object is always a usage
    val firstLastUsages = this.getTable("uses").select("idx", "callee")
      .groupBy(_.getLong("callee"))
      .map(
        {
          case (id, rows) =>
            (id, (rows.map(_.getLong("idx")).min, rows.map(_.getLong("idx")).max))
        })
      .filter(_._1 != 0)

    val joined = firstLastCalls.fullOuterJoin(firstLastUsages)
      .map({
        case (id, (None, Some(x))) => (id, x)
        case (id, (Some(x), None)) => (id, x)
        case (id, (Some((min1, max1)), Some((min2, max2)))) =>
          (id, (Math.min(min1, min2), Math.max(max1, max2)))
      }).collect()

    assert(joined.find(_._1 == 0).isEmpty, "can not have id = 0")

    for (firstLastUsage <- joined) {
      firstLastUsage match {
        case (id, (fst, lst)) =>
          assert(id != 0, "id must not be 0")
          this.session.execute(
            "  UPDATE objects " +
              "SET " +
              s"  firstUsage = ${fst}, " +
              s"  lastUsage  = ${lst}" +
              "WHERE id = " + id)
      }
    }
  }

  def generatePerClassObjectsTable(): Unit = {
    getTable("objects").collect().foreach(row => {
      val id = row.getLong("id").asInstanceOf[java.lang.Long]
      val klass = row.getStringOption("klass")
      val allocationsitefile = row.getStringOption("allocationsitefile").getOrElse(null).asInstanceOf[String]
      val allocationsiteline = row.getLongOption("allocationsiteline").getOrElse(null).asInstanceOf[java.lang.Long]
      val firstUsage = row.getLong("firstusage").asInstanceOf[java.lang.Long]
      val lastUsage = row.getLong("lastusage").asInstanceOf[java.lang.Long]
      val thread = row.getStringOption("thread").getOrElse(null)
      val comment = row.getStringOption("comment").getOrElse(null)

      this.session.execute("INSERT INTO objects_per_class (" +
        "id, klass, allocationsitefile, allocationsiteline, firstusage, " +
        "lastusage, thread, comment) VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
        id, klass.getOrElse("<unknown class>"), allocationsitefile, allocationsiteline, firstUsage, lastUsage,
        thread, comment)
    })
  }

  def storeClassFile(logDir: Path, file: File) {
    val relPath = logDir.relativize(file.toPath)

    val segments = for (i <- 0 until relPath.getNameCount) yield relPath.getName(i)
    val className = segments.mkString(".").replace(".class", "")
    print(s"loading $className...")
    val fileChannel = new RandomAccessFile(file, "r").getChannel
    val bytebuffer: ByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size)

    this.session.execute(
      "INSERT INTO "+this.session.getLoggedKeyspace+".classdumps" +
        " (classname, bytecode) VALUES (?, ?);", className, bytebuffer)

    println(" done")
  }

  def loadBytecodeDir(logDir: Path) = {
    val files = FileUtils.listFiles(logDir.toFile, null, true)
    for (f <- files.iterator()) {
      storeClassFile(logDir, f.asInstanceOf[File])
    }
  }


  def loadFrom(path: String, logDir: Path) {

    println(s"loading from $path, logDir=$logDir")

    var hadLateInits = false
    var doneWithHandleInits = false
    val stopwatch: Stopwatch = Stopwatch.createStarted

    this.connect(true)

    this.loadBytecodeDir(Paths.get(logDir.toString, "input"))

    val events = TraceFiles.fromPath(path).iterator
    var i : Long = 1
    while (events.hasNext) {
      val evt: AnyEvt.Reader = events.next

      if (evt.which() == AnyEvt.Which.LATEINIT) {
        hadLateInits = true
      } else if (hadLateInits) {
        doneWithHandleInits = true
      }

      if (doneWithHandleInits) {
        if (!Util.isTrickyThreadName(EventsUtil.getThread(evt))) {
          handleEvent(evt, i)
        }
        if ((i-1) % 1e5 == 0) {
          println("#" + ((i-1) / 1e6).toFloat + "e6..")
        }
      }
      i += 1
    }
    println("loading "+(i-1)+" events took "+stopwatch.stop())
    sortConstructorCalls()
    computeEdgeEnds()
    computeLastObjUsages()
    generatePerClassObjectsTable()
    this.shutdown()
  }

  def connect(overwrite: Boolean = false): Unit = {

    SpencerDB.initCluster()
    if (overwrite) {
      initKeyspace(this.keyspace)
    }

    initPreparedStatements(this.keyspace)

    connectToKeyspace(this.keyspace)

    SpencerDB.startSpark()
  }

  def connectToKeyspace(keyspace: String): Unit = {
    this.session = SpencerDB.cluster.connect(keyspace)
  }

  def initKeyspace(keyspace: String) {
    val session = SpencerDB.cluster.connect()
    session.execute("DROP KEYSPACE IF EXISTS " + keyspace + ";")
    session.execute(
      s"CREATE KEYSPACE $keyspace WITH replication = {"
        + " 'class': 'SimpleStrategy',"
        + " 'replication_factor': '1'"
        + "};")

    session.execute(s"CREATE TABLE $keyspace.snapshots(query text, result blob, PRIMARY KEY(query));")

    session.execute(s"CREATE TABLE $keyspace.classdumps(classname text, bytecode blob, PRIMARY KEY(classname));")

    session.execute(s"CREATE TABLE $keyspace.objects(" +
      "id bigint, " +
      "klass text, " +
      "allocationsitefile text, " +
      "allocationsiteline bigint, " +
      "firstUsage bigint, " +
      "lastUsage bigint, " +
      "thread text, " +
      "comment text, " +
      "PRIMARY KEY(id)) " +
      "WITH COMPRESSION = {};")

    session.execute(s"CREATE INDEX on $keyspace.objects(allocationsiteline);")
    session.execute(s"CREATE INDEX on $keyspace.objects(allocationsitefile);")

    session.execute(s"CREATE TABLE $keyspace.objects_per_class(" +
      "id bigint, " +
      "klass text, " +
      "allocationsitefile text, " +
      "allocationsiteline bigint, " +
      "firstUsage bigint, " +
      "lastUsage bigint, " +
      "thread text, " +
      "comment text, " +
      // this table supports range queries on id, firstUsage, lastUsage:
      "PRIMARY KEY(klass, id, firstUsage, lastUsage)) " +
      "WITH COMPRESSION = {};")

    session.execute(s"CREATE TABLE $keyspace.refs(" +
      "caller bigint, callee bigint, " +
      "kind text, "+
      "name text, "+
      "start bigint, end bigint, " +
      "thread text, " +
      "comment text, " +
      "PRIMARY KEY(caller, kind, start)) " +
      "WITH COMPRESSION = {};")

    // allows to select efficiently using WHERE kind = 'var' in computeEdgeEnds
    session.execute(s"CREATE INDEX ON $keyspace.refs (KIND);")

    session.execute(s"CREATE TABLE $keyspace.calls(" +
      "caller bigint, callee bigint, " +
      "name text, " +
      "start bigint, end bigint, " +
      "callsitefile text, callsiteline bigint, " +
      "thread text, " +
      "comment text, "+
      "PRIMARY KEY(caller, callee, start, end)) " +
      "WITH COMPRESSION = {};")

    session.execute(s"CREATE TABLE $keyspace.uses(" +
      "caller bigint, callee bigint, name text, method text, kind text, idx bigint, thread text, comment text, " +
      "PRIMARY KEY(caller, callee, idx)" +
      ") " +
      "WITH COMPRESSION = {};")

    session.close()
  }

  def initPreparedStatements(keyspace: String): Unit = {
    this.session = SpencerDB.cluster.connect(keyspace)

    this.insertObjectStatement = this.session.prepare("INSERT INTO objects(id, klass, allocationSiteFile, allocationSiteLine, thread, comment) VALUES(?, ?, ?, ?, ?, ?);")
    this.insertEdgeStatement = this.session.prepare(s"INSERT INTO ${this.keyspace}.refs(caller, callee, kind, name, thread, start, end, comment) VALUES(?, ?, ?, ?, ?, ?, ?, ?);")
    this.insertEdgeOpenStatement = this.session.prepare(s"INSERT INTO ${this.keyspace}.refs(caller, callee, kind, name, thread, start, comment) VALUES(?, ?, ?, ?, ?, ?, ?);")
    this.finishEdgeStatement = this.session.prepare(s"UPDATE ${this.keyspace}.refs SET end = ? WHERE kind = ? AND start = ? AND caller = ?;")

    this.insertCallStatement = this.session.prepare(s"INSERT INTO ${this.keyspace}.calls(caller, callee, name, start, end, thread, callsitefile, callsiteline, comment) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?);")
    this.insertUseStatement = this.session.prepare(s"INSERT INTO ${this.keyspace}.uses(caller, callee, method, kind, name, idx, thread, comment) VALUES(?, ?, ?, ?, ?, ?, ?, ?);")
  }
}

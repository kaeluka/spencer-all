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

class SpencerDB(val keyspace: String) {
  def shutdown() = {
    this.session.close()
    this.cluster.close()
    this.sc.stop()
  }

  var cluster : Cluster = _

  var insertUseStatement : PreparedStatement = _
  var insertEdgeStatement : PreparedStatement = _
  var finishEdgeStatement : PreparedStatement = _
  var insertCallStatement : PreparedStatement = _
  var insertObjectStatement : PreparedStatement = _
  var session : Session = _
  val saveOrigin = true
  var sc : SparkContext = _

  val stacks: CallStackAbstraction = new CallStackAbstraction()

  def startSpark() {
    if (this.isSparkStarted) {
      throw new AssertionError()
    }
    val conf = new SparkConf()
      .setAppName("spencer-analyse")
      .set("spark.cassandra.connection.host", "127.0.0.1")
//      .set("spark.cassandra.connection.host", "130.238.10.30")
//      .setMaster("spark://Stephans-MacBook-Pro.local:7077")
//      .set("spark.executor.memory", "4g").set("worker_max_heap", "1g")
      .setMaster("local[8]")

    this.sc = new SparkContext(conf)
//    this.sc.setCheckpointDir("/Volumes/MyBook/checkpoints")
  }

  def isSparkStarted : Boolean = {
    this.sc != null
  }

  def handleEvent(evt: AnyEvt.Reader, idx: Long) {
    try {
//      if (EventsUtil.messageToString(evt).contains("91978")) {
//        println("#" + idx + ": " + EventsUtil.messageToString(evt))
//      }
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
          insertEdge(
            caller = fstore.getHoldertag,
            callee = fstore.getNewval,
            kind = "field",
            name = fstore.getFname.toString,
            thread = fstore.getThreadName.toString,
            start = idx,
            comment = EventsUtil.messageToString(evt))

        case AnyEvt.Which.METHODENTER =>
          val menter = evt.getMethodenter
          stacks.push(menter, idx)

          if (menter.getName.toString == "<init>") {
            insertObject(menter.getCalleetag, menter.getCalleeclass.toString,
              menter.getCallsitefile.toString, menter.getCallsiteline,
              EventsUtil.messageToString(evt))
          }
        case AnyEvt.Which.METHODEXIT =>
          val mexit = evt.getMethodexit
          try {
            val calleeTag: Long = stacks.peek(mexit.getThreadName.toString) match {
              case Some(idxdEnter) => idxdEnter.enter.getCalleetag
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
                  caller = callerTag,
                  callee = calleeTag,
                  name = menter.enter.getName.toString,
                  start = menter.idx,
                  end = idx,
                  thread = menter.enter.getThreadName.toString,
                  callSiteFile = menter.enter.getCallsitefile.toString,
                  callSiteLine = menter.enter.getCallsiteline,
                  comment = "")
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
            insertEdge(lateinit.getCalleetag, fld.getVal, "field", fld.getName.toString, "<JVM thread>", 1, "")
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
          insertUse(caller, callee,
            stacks.peek(readmodify.getThreadName.toString).map(_.enter.getName.toString).getOrElse("<unknown>"),
            kind,
            readmodify.getFname.toString,
            idx,
            readmodify.getThreadName.toString,
            if (saveOrigin) EventsUtil.messageToString(evt) else "")
        case AnyEvt.Which.VARLOAD =>
          val varload: Events.VarLoadEvt.Reader = evt.getVarload
          insertUse(varload.getCallertag,
            varload.getVal,
            varload.getCallermethod.toString,
            "varload",
            "var_"+varload.getVar.toString,
            idx,
            varload.getThreadName.toString,
            EventsUtil.messageToString(evt))
        case AnyEvt.Which.VARSTORE =>
          val varstore: Events.VarLoadEvt.Reader = evt.getVarload
          insertEdge(
            varstore.getCallertag,
            varstore.getVal,
            "var",
            "var_"+varstore.getVar,
            varstore.getThreadName.toString,
            idx,
            EventsUtil.messageToString(evt))
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

  def insertObject(tag: Long, klass: String, allocationsitefile: String, allocationsiteline: Long, comment: String = "none") {
    //    if (! session.execute("SELECT id FROM "+this.keyspace+".objects WHERE id = "+tag).isExhausted) {
    //      throw new IllegalStateException("already have object #"+tag);
    //    }
    //      session.executeAsync("INSERT INTO objects(id, klass, comment) VALUES("
    //        + tag + ", '"
    //        + klass + "', '"
    //        + comment + "');")
    assert(allocationsitefile != null)
    session.executeAsync(this.insertObjectStatement.bind(
      tag : java.lang.Long,
      klass.replace('/','.'),
      allocationsitefile,
      allocationsiteline : java.lang.Long,
      if (saveOrigin) comment else ""))
  }

  def insertEdge(caller: Long, callee: Long, kind: String, name: String, thread: String, start: Long, comment: String = "none") {
    session.executeAsync(this.insertEdgeStatement.bind(
      caller : java.lang.Long,
      callee : java.lang.Long,
      kind,
      name,
      thread,
      start  : java.lang.Long,
      if (saveOrigin) comment else ""))
  }

  def insertUse(caller: Long, callee: Long, method: String, kind: String, name: String, idx: Long, thread: String, comment: String = "none") {

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
      usesTable.filter(row => row.getLong("caller") == tag || row.getLong("callee") == tag)
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
    val calls =
      callsTable.filter(row => row.getLong("caller") == tag || row.getLong("callee") == tag)
        .map(row =>
          (row.getLong("caller")
            , row.getLong("callee")
            , row.getLong("start")
            , row.getLong("end")
            , row.getString("name")
            , row.getString("callsitefile") + ":" + row.getLong("callsiteline")
            , row.getStringOption("thread").getOrElse("<unknown thread>")
            , row.getString("comment"))
        )

    val callsEvents = calls.map {
      case (caller, callee, start, end, name, callsite, thread, comment) =>
        AproposCallEvent(caller, callee, start, end, name, callsite, thread, "call "+comment).asInstanceOf[AproposEvent]
    }

    val refsTable = this.getTable("refs")
    val refs =
      (refsTable.where("caller = ?", tag) ++
        refsTable.where("callee = ?", tag)).map(row =>
        (
          row.getLong("caller")
          , row.getLong("callee")
          , row.getLong("start")
          , row.getLongOption("end")
          , row.getString("name")
          , row.getString("kind")
          , row.getStringOption("thread").getOrElse("<unknown thread>")
          , row.getString("comment")))

    val refsEvents = refs.map {
      case (caller, callee, start, end, name, kind, thread, comment) =>
        AproposRefEvent(caller, callee, start, end, name, kind, thread, comment).asInstanceOf[AproposEvent]
    }

    val alloc = if (theObj.count > 0) {
      Some(theObj.collect()(0).toMap)
    } else {
      None
    }
    AproposData(
      alloc,
      (useEvents++callsEvents++refsEvents)
        .sortBy(AproposEvent.startTime(_)).collect(), klass)
  }

  def getTable(table: String): CassandraTableScanRDD[CassandraRow] = {
    this.sc.cassandraTable(this.session.getLoggedKeyspace, table)
  }

  def getProperObjects: RDD[CassandraRow] = {
    getTable("objects")
      .filter(_.getLong("id") > 0)
      .filter(
        _.getStringOption("comment")
          .forall(!_.contains("late initialisation")))
  }

  def computeEdgeEnds(): Unit = {
    val groupedAssignments = this.getTable("refs")
      .groupBy(row => (row.getLong("caller"), row.getStringOption("name")))
      .filter(_._1._2.nonEmpty)
      .map({case ((caller, Some(name)), rows) =>
        val sorted =
          rows.toSeq.sortBy(_.getLong("start"))
            .map(row =>
              (row.getLong("start"), row.getLong("callee").asInstanceOf[VertexId], row.getString("kind"))
            )
        (caller, name, sorted)
      }).collect()

    var cnt = 0
    for ((caller, name, assignments) <- groupedAssignments) {
      for (i <- 1 to (assignments.size-2)) {
        (assignments(i), assignments(i+1)) match {
          case ((start, callee, kind), (end, _, _)) =>
            session.executeAsync(this.finishEdgeStatement.bind(end : java.lang.Long, kind, start : java.lang.Long, caller : java.lang.Long))
            cnt = cnt + 1
        }
      }
    }
    println(cnt + " assignments")
//    continue here:
      // groupedAssignments contains now the sequence of values that were stored in each object x field.
  }

  def computeLastObjUsages(): Unit = {
    if (! this.isSparkStarted) {
      throw new AssertionError()
    }

    //the first action involving an object is always its constructor call
    val firstLastCalls = this.getTable("calls")
      .select("callee", "start", "end")
      .groupBy(_.getLong("callee"))
      .map {
        case (id, calls) =>
          (id, (calls.map(_.getLong("start")).min, calls.map(_.getLong("end")).max))
      }


//    continuehere: object #91978 has no first/last usage

    //the last action involving an object is always a usage
    val firstLastUsages = this.getTable("uses").select("idx", "callee")
      .groupBy(_.getLong("callee"))
      .map(
        {
          case (id, rows) =>
            (id, (rows.map(_.getLong("idx")).min, rows.map(_.getLong("idx")).max))
        })


    val joined = firstLastCalls.fullOuterJoin(firstLastUsages)
      .map({
        case (id, (None, Some(x))) => (id, x)
        case (id, (Some(x), None)) => (id, x)
        case (id, (Some((min1, max1)), Some((min2, max2)))) =>
          (id, (Math.min(min1, min2), Math.max(max1, max2)))
      }).collect()
//    val joined = firstLastCalls.join(firstLastUsages)
//      .map({
//        case (id, ((min1, max1), (min2, max2))) =>
//          (id, (Math.min(min1, min2), Math.max(max1, max2)))
//      }).collect()

    for (firstLastUsage <- joined) {
      firstLastUsage match {
        case (id, (fst, lst)) =>
          this.session.execute(
            "  UPDATE objects " +
              "SET " +
              "  firstUsage = " + fst + ", " +
              "  lastUsage  = " + lst +
              "WHERE id = " + id)
      }
    }
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
        i += 1
      }
    }
    println("loading "+(i-1)+" events took "+stopwatch.stop())
    computeEdgeEnds()
    computeLastObjUsages()
    sc.stop()
  }

  def connect(overwrite: Boolean = false): Unit = {
    this.cluster =
      Cluster.builder()
        .addContactPoint("127.0.0.1")
//        .addContactPoint("130.238.10.30")
        .build()

    if (overwrite) {
      initKeyspace(cluster, this.keyspace)
    }

    initPreparedStatements(cluster, this.keyspace)

    connectToKeyspace(cluster, this.keyspace)

    this.startSpark()
  }

  def connectToKeyspace(cluster: Cluster, keyspace: String): Unit = {
    this.session = cluster.connect(keyspace)
  }

  def initKeyspace(cluster: Cluster, keyspace: String) {
    val session = cluster.connect()
    session.execute("DROP KEYSPACE IF EXISTS " + keyspace + ";")
    session.execute(
      "CREATE KEYSPACE " + keyspace+ " WITH replication = {"
        + " 'class': 'SimpleStrategy',"
        + " 'replication_factor': '1'"
        + "};")

    session.execute("CREATE TABLE "+keyspace+".snapshots(query text, result blob, PRIMARY KEY(query));")

    session.execute("CREATE TABLE "+keyspace+".classdumps(classname text, bytecode blob, PRIMARY KEY(classname));")

    session.execute("CREATE TABLE "+keyspace+".objects(" +
      "id bigint, " +
      "klass text, " +
      "allocationsitefile text, " +
      "allocationsiteline bigint, " +
      "firstUsage bigint, " +
      "lastUsage bigint, " +
      "comment text, " +
      "PRIMARY KEY(id)) " +
      "WITH COMPRESSION = {};")

    session.execute("CREATE TABLE "+keyspace+".refs(" +
      "caller bigint, callee bigint, " +
      "kind text, "+
      "name text, "+
      "start bigint, end bigint, " +
      "thread text, " +
      "comment text, " +
      "PRIMARY KEY(caller, kind, start)) " +
      "WITH COMPRESSION = {};")

    session.execute("CREATE TABLE "+keyspace+".calls(" +
      "caller bigint, callee bigint, " +
      "name text, " +
      "start bigint, end bigint, " +
      "callsitefile text, callsiteline bigint, " +
      "thread text, " +
      "comment text, "+
      "PRIMARY KEY(caller, callee, start, end)) " +
      "WITH COMPRESSION = {};")

    session.execute("CREATE TABLE "+keyspace+".uses(" +
      "caller bigint, callee bigint, name text, method text, kind text, idx bigint, thread text, comment text, " +
      "PRIMARY KEY(caller, callee, idx)" +
      ") " +
      "WITH COMPRESSION = {};")

    session.close()
  }

  def initPreparedStatements(cluster: Cluster, keyspace: String): Unit = {
    this.session = cluster.connect(keyspace)

    this.insertObjectStatement = this.session.prepare("INSERT INTO objects(id, klass, allocationSiteFile, allocationSiteLine, comment) VALUES(?, ?, ?, ?, ?);")
    this.insertEdgeStatement = this.session.prepare("INSERT INTO " + this.keyspace + ".refs(caller, callee, kind, name, thread, start, comment) VALUES(?, ?, ?, ?, ?, ?, ?);")
    this.finishEdgeStatement = this.session.prepare("UPDATE " + this.keyspace + ".refs SET end = ? WHERE kind = ? AND start = ? AND caller = ?;")

    this.insertCallStatement = this.session.prepare("INSERT INTO "
      + this.keyspace + ".calls(caller, callee, name, start, end, thread, callsitefile, callsiteline, comment) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?);")
    this.insertUseStatement = this.session.prepare("INSERT INTO " + this.keyspace + ".uses(caller, callee, method, kind, name, idx, thread, comment) VALUES(?, ?, ?, ?, ?, ?, ?, ?);")
  }
}

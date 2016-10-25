package com.github.kaeluka.spencer.tracefiles

import java.io.{File, FileInputStream, InputStream}
import java.util.EmptyStackException

import com.datastax.driver.core._
import com.datastax.spark.connector.{CassandraRow, _}
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import com.github.kaeluka.spencer.Events
import com.github.kaeluka.spencer.Events.{AnyEvt, LateInitEvt, ReadModifyEvt}
import com.github.kaeluka.spencer.analysis.{CallStackAbstraction, IndexedEnter, Util}
import com.google.common.base.Stopwatch
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

case class AproposUseEvent(idx: Long, caller: VertexId, callee: VertexId, msg: String)
case class AllocationRecord(msg: String)

case class AproposData(alloc: AllocationRecord, evts: Seq[AproposUseEvent])

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
            idx,
            EventsUtil.messageToString(evt))
        case AnyEvt.Which.FIELDSTORE =>
          val fstore = evt.getFieldstore
          insertEdge(fstore.getHoldertag, fstore.getNewval, "field", fstore.getFname.toString, idx, EventsUtil.messageToString(evt))

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
              case Left(x) => throw new AssertionError(x.toString)
              case Right(menter) =>
                val callerTag: Long = stacks.peek(mexit.getThreadName.toString) match {
                  case Some(t) => t.enter.getCalleetag
                  case None => 4 // SPECIAL_VAL_JVM
                }
                val caller: Unit = insertCall(
                  callerTag, calleeTag,
                  menter.enter.getName.toString,
                  menter.idx, idx,
                  menter.enter.getCallsitefile.toString, menter.enter.getCallsiteline,
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
            insertEdge(lateinit.getCalleetag, fld.getVal, "field", null, 1, "name = "+fld.getName)
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
            idx,
            EventsUtil.messageToString(evt))
        case AnyEvt.Which.VARLOAD =>
        //        val varload: Events.VarLoadEvt.Reader = evt.getVarload
        //        insertEdge(session, varload.getCallertag, varload.getVal, "varload", idx, idx+1)
        case AnyEvt.Which.VARSTORE =>
          val varstore: Events.VarLoadEvt.Reader = evt.getVarload
          insertEdge(varstore.getCallertag, varstore.getVal, "var", "var_"+varstore.getVar, idx, EventsUtil.messageToString(evt))
        case other =>
          throw new IllegalStateException(
            "do not know how to handle event kind " + other)
      }
    } catch {
      case e: IllegalStateException =>
        val exception: IllegalStateException = new IllegalStateException("cause was: "+EventsUtil.messageToString(evt), e)
    }
  }

  def insertCall(caller: Long, callee: Long, name: String, start : Long, end: Long, callSiteFile: String, callSiteLine: Long, comment: String = "none") {
    session.executeAsync(this.insertCallStatement.bind(
      caller : java.lang.Long, callee : java.lang.Long,
      name,
      start : java.lang.Long, end : java.lang.Long,
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

  def insertEdge(caller: Long, callee: Long, kind: String, name: String, start: Long, comment: String = "none") {
    session.executeAsync(this.insertEdgeStatement.bind(
      caller : java.lang.Long,
      callee : java.lang.Long,
      kind,
      name,
      start  : java.lang.Long,
      if (saveOrigin) comment else ""))
  }

  def insertUse(caller: Long, callee: Long, method: String, kind: String, idx: Long, comment: String = "none") {

    session.executeAsync(this.insertUseStatement.bind(
      caller : java.lang.Long,
      callee : java.lang.Long,
      method,
      kind,
      idx : java.lang.Long,
      if (saveOrigin) comment else ""))
  }


  def aproposObject(tag: Long): AproposData = {
    val objTable: CassandraTableScanRDD[CassandraRow] = getTable("objects")

    val theObj : RDD[CassandraRow] = objTable
      .filter(_.getLong("id") == tag)
      .cache
//    theObj
//      .map(_.getStringOption("klass"))
//      .collect

    var allocMsg  = ""
    if (theObj.count == 0) {
      allocMsg += "<not initialised>\n"
    } else {
      allocMsg += theObj.collect.mkString(", ")+"\n"
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
          (row.getLong("idx"), row.getLong("caller"), row.getLong("callee"), row.getString("comment"))
//            "object "+row.getLong("caller")+", method "+row.getString("method")+":\t"+row.getString("kind")+"\tof object\t"+row.getLong("callee"))
        )
        .sortBy(_._1)

    val useEvents = uses.collect.map {
      case ((idx, caller, callee, comment)) => AproposUseEvent(idx, caller, callee, comment)
    }

//    val refsTable: CassandraTableScanRDD[CassandraRow] = this.getTable("refs")
//    val refs : RDD[CassandraRow] =
//      refsTable.where("caller = ?", tag) ++
//        refsTable.where("callee = ?", tag)
//
//    ret += "references from/to the object:\n  - "
//    if (refs.count == 0) {
//      ret += "<no references>\n"
//    } else {
//      ret += refs.collect.mkString("\n  - ")+"\n"
//    }
//
//    ret.replaceAll(" "+tag+" ", " <THE OBJECT>")

    AproposData(AllocationRecord(allocMsg), useEvents)

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
      .map({case ((caller, Some(name)), rows) => {
        val sorted =
          rows.toSeq.sortBy(_.getLong("start"))
            .map(row =>
              (row.getLong("start"), row.getLong("callee").asInstanceOf[VertexId], row.getString("kind"))
            )
        (caller, name, sorted)
      }}).collect()

    println("hello fields: "+groupedAssignments.filter(_._1 == -91972).mkString("[",", ", "]"))

    var cnt = 0
    for ((caller, name, assignments) <- groupedAssignments) {
      if (caller == -91972) {
        println((caller, name, assignments))
      }
      for (i <- 1 to (assignments.size-2)) {
        (assignments(i), assignments(i+1)) match {
          case ((start, callee, kind), (end, _, _)) => {
//            println((end, kind, start, caller))
            session.executeAsync(this.finishEdgeStatement.bind(end : java.lang.Long, kind, start : java.lang.Long, caller : java.lang.Long))
            cnt = cnt + 1
          }
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
      .map({
        case (id, calls) =>
          (id, (calls.map(_.getLong("start")).min, calls.map(_.getLong("end")).max))
      })


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


  def loadFrom(path: String) {

    var hadLateInits = false
    var doneWithHandleInits = false
    val stopwatch: Stopwatch = Stopwatch.createStarted

    this.connect(true)

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

    session.execute("CREATE TABLE "+keyspace+".snapshots(query text, result text, PRIMARY KEY(query));")

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
      "comment text, " +
      "PRIMARY KEY(caller, kind, start)) " +
      "WITH COMPRESSION = {};")

    session.execute("CREATE TABLE "+keyspace+".calls(" +
      "caller bigint, callee bigint, " +
      "name text, " +
      "start bigint, end bigint, " +
      "callsitefile text, callsiteline bigint, " +
      "comment text, "+
      "PRIMARY KEY(caller, callee, start, end)) " +
      "WITH COMPRESSION = {};")

    session.execute("CREATE TABLE "+keyspace+".uses(" +
      "caller bigint, callee bigint, method text, kind text, idx bigint, comment text, " +
      "PRIMARY KEY(caller, callee, idx)" +
      ") " +
      "WITH COMPRESSION = {};")

    session.close()
  }

  def initPreparedStatements(cluster: Cluster, keyspace: String): Unit = {
    this.session = cluster.connect(keyspace)

    this.insertObjectStatement = this.session.prepare("INSERT INTO objects(id, klass, allocationSiteFile, allocationSiteLine, comment) VALUES(?, ?, ?, ?, ?);")
    this.insertEdgeStatement = this.session.prepare("INSERT INTO " + this.keyspace + ".refs(caller, callee, kind, name, start, comment) VALUES(?, ?, ?, ?, ?, ?);")
    this.finishEdgeStatement = this.session.prepare("UPDATE " + this.keyspace + ".refs SET end = ? WHERE kind = ? AND start = ? AND caller = ?;")

    this.insertCallStatement = this.session.prepare("INSERT INTO "
      + this.keyspace + ".calls(caller, callee, name, start, end, callsitefile, callsiteline, comment) VALUES(?, ?, ?, ?, ?, ?, ?, ?);")
    this.insertUseStatement = this.session.prepare("INSERT INTO " + this.keyspace + ".uses(caller, callee, method, kind, idx, comment) VALUES(?, ?, ?, ?, ?, ?);")
  }
}

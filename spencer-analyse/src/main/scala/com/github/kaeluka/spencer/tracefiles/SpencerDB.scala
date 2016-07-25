package com.github.kaeluka.spencer.tracefiles

import java.io.{File, FileInputStream}
import java.util.concurrent.ConcurrentHashMap

import com.datastax.driver.core._
import com.datastax.spark.connector.CassandraRow
import com.github.kaeluka.spencer.{DBLoader, Events}
import com.github.kaeluka.spencer.Events.{AnyEvt, LateInitEvt, MethodEnterEvt, ReadModifyEvt}
import com.google.common.base.Stopwatch
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import com.github.kaeluka.spencer.analysis.CallStackAbstraction
import org.apache.spark.rdd.RDD

/**
  * Created by stebr742 on 2016-07-01.
  */
class SpencerDB(val keyspace: String) {
  var insertUseStatement : PreparedStatement = null;
  var insertEdgeStatement : PreparedStatement = null
  var insertObjectStatement : PreparedStatement = null
  var session : Session = null
  val saveOrigin = true
  var sc : SparkContext = null;

  val stacks: CallStackAbstraction = new CallStackAbstraction()

  def startSpark() {
    if (this.isSparkStarted) {
      throw new AssertionError()
    }
    val conf = new SparkConf()
      .setAppName("spencer-analyse")
      .set("spark.cassandra.connection.host", "127.0.0.1")
      .setMaster("local[2]")

    this.sc = new SparkContext(conf)
  }

  def isSparkStarted : Boolean = {
    this.sc != null
  }

  def handleEvent(evt: AnyEvt.Reader, idx: Int) {
    try {
      evt.which() match {
        case AnyEvt.Which.FIELDLOAD =>
          val fieldload = evt.getFieldload
          insertUse(fieldload.getCallertag, fieldload.getHoldertag, "fieldload", idx, EventsUtil.messageToString(evt))
        case AnyEvt.Which.FIELDSTORE =>
          val fstore = evt.getFieldstore
          insertEdge(fstore.getCallertag, fstore.getHoldertag, "field", idx, EventsUtil.messageToString(evt))
        case AnyEvt.Which.METHODENTER =>
          val menter = evt.getMethodenter
          val oldTop: MethodEnterEvt.Reader = stacks.peek(menter.getThreadName.toString).get.enter
          stacks.push(menter, idx)
          if (menter.getName.toString == "<init>") {
            //            if (menter.getCallsiteline != -1) {
            //              println("inserting: " + menter.getCalleetag)
            //            }
            insertObject(menter.getCalleetag, menter.getCalleeclass.toString, menter.getCallsitefile.toString, menter.getCallsiteline, EventsUtil.messageToString(evt))
          }
          insertUse(oldTop.getCalleetag, menter.getCalleetag, "call", idx, EventsUtil.messageToString(evt))
        case AnyEvt.Which.METHODEXIT =>
          stacks.pop(evt.getMethodexit)
        case AnyEvt.Which.LATEINIT =>
          val lateinit: LateInitEvt.Reader = evt.getLateinit
          insertObject(lateinit.getCalleetag
            , lateinit.getCalleeclass.toString
            , "<jvm internals>"
            , -1, "late initialisation")
        case AnyEvt.Which.READMODIFY =>

          val readmodify: ReadModifyEvt.Reader = evt.getReadmodify
          val caller: Long = readmodify.getCallertag
          val callee: Long = readmodify.getCalleetag
          val kind: String = if (readmodify.getIsModify) {
            "modify"
          } else {
            "read"
          }
          insertUse(caller, callee, kind, idx, EventsUtil.messageToString(evt))
        //        val readmodify = evt.getReadmodify
        //        insertEdge(session, readmodify.getCallertag, readmodify.getCalleetag,
        //          if (readmodify.getIsModify) { "modify" } else { "read" }, idx, idx+1)
        case AnyEvt.Which.VARLOAD =>
        //        val varload: Events.VarLoadEvt.Reader = evt.getVarload
        //        insertEdge(session, varload.getCallertag, varload.getVal, "varload", idx, idx+1)
        case AnyEvt.Which.VARSTORE =>
          val varstore: Events.VarLoadEvt.Reader = evt.getVarload
          insertEdge(varstore.getCallertag, varstore.getVal, "var", idx, EventsUtil.messageToString(evt))
        case other =>
          throw new IllegalStateException(
            "do not know how to handle event kind " + other)
      }
    } catch {
      case e: IllegalStateException =>
        val exception: IllegalStateException = new IllegalStateException("cause was: "+EventsUtil.messageToString(evt), e)
    }
  }

  def insertObject(tag: Long, klass: String, allocationSiteFile: String, allocationSiteLine: Long, comment: String = "none") {
    //    if (! session.execute("SELECT id FROM "+this.keyspace+".objects WHERE id = "+tag).isExhausted) {
    //      throw new IllegalStateException("already have object #"+tag);
    //    }
    //      session.executeAsync("INSERT INTO objects(id, klass, comment) VALUES("
    //        + tag + ", '"
    //        + klass + "', '"
    //        + comment + "');")
    session.executeAsync(this.insertObjectStatement.bind(
      tag : java.lang.Long,
      klass,
      allocationSiteFile,
      allocationSiteLine : java.lang.Long,
      if (saveOrigin) comment else null))
  }

  def insertEdge(caller: Long, callee: Long, kind: String, start: Long, comment: String = "none") {
    session.executeAsync(this.insertEdgeStatement.bind(
      caller : java.lang.Long,
      callee : java.lang.Long,
      kind,
      start  : java.lang.Long,
      new java.lang.Long(-1),
      if (saveOrigin) comment else null))
  }

  def insertUse(caller: Long, callee: Long, kind: String, idx: Long, comment: String = "none") {

    session.executeAsync(this.insertUseStatement.bind(
      caller : java.lang.Long,
      callee : java.lang.Long,
      kind,
      idx : java.lang.Long,
      if (saveOrigin) comment else null))
  }

  def aproposObject(tag: Long): String = {
    val objTable: CassandraTableScanRDD[CassandraRow] = getTable("objects")

    val theObj : RDD[CassandraRow] = objTable
      .select("klass", "allocationsitefile", "allocationsiteline")
      .where("id = ?", tag)
      .cache
    theObj
      .map(_.getString("klass"))
      .collect

    var ret = tag+ " :: "
    if (theObj.count == 0) {
      ret += "<not initialised>\n"
    } else {
      ret += theObj.collect.mkString(", ")+"\n"
      ret += "allocated at:\n  - "
      ret +=
        theObj
          .map(row => row.getString("allocationsitefile")++"::"++row.getString("allocationsiteline"))
          .collect
        .mkString("\n  - ")
      ret += "\n"
    }

    val usesTable: CassandraTableScanRDD[CassandraRow] = this.getTable("uses")
    val uses: RDD[CassandraRow] =
      usesTable.select("comment").where("caller = ?", tag) ++
        usesTable.select("comment").where("callee = ?", tag)

    ret += "uses:\n  "
    if (uses.count == 0) {
      ret += "  <no uses>\n"
    } else {
      ret += uses.map(_.getString("comment")).collect.mkString("\n  ")+"\n"
    }

    val refsTable: CassandraTableScanRDD[CassandraRow] = this.getTable("refs")
    val refs : RDD[CassandraRow] =
      refsTable.select("comment").where("caller = ?", tag) ++
        refsTable.select("comment").where("callee = ?", tag)

    ret += "references from/to the object:\n  - "
    if (refs.count == 0) {
      ret += "<no references>\n"
    } else {
      ret += refs.map(_.getString("comment")).collect.mkString("\n  - ")+"\n"
    }

    ret.replaceAll(" "+tag+" ", " <THE OBJECT>")

  }

  def getTable(table: String): CassandraTableScanRDD[CassandraRow] = {
    this.sc.cassandraTable(this.session.getLoggedKeyspace, table)
  }

  def getLastObjUsages(): Unit = {
    if (! this.isSparkStarted) {
      throw new AssertionError()
    }
    val watch: Stopwatch = Stopwatch.createStarted()

    val lastUsages = sc.cassandraTable(this.session.getLoggedKeyspace, "uses")
      .select("idx", "callee")
      .groupBy(_.getLong("callee"))
      .map(
        { case (_, rows) =>
          (rows.minBy(_.getLong("idx")), rows.maxBy(_.getLong("idx")))}
      ).collect()

    watch.stop()

    println("computing last usages took "+watch)

    watch.reset()
    watch.start()
    for (lastUsage <- lastUsages) {
      lastUsage match {
        case (fst, lst) =>
          assert(fst.getLong("callee") == lst.getLong("callee"))
          this.session.execute(
            "  UPDATE objects " +
              "SET " +
              "  firstUsage = " + fst.getLong("idx") + ", " +
              "  lastUsage  = " + lst.getLong("idx") +
              "WHERE id = " + fst.getLong("callee"))
      }
    }
    watch.stop()
    println("updating last usages took "+watch)
    sc.stop()
  }

  def loadFrom(f : File) {

    val stopwatch: Stopwatch = Stopwatch.createStarted

    this.connect(true)

    val events = new TraceFile(f).iterator
    var i = 1
    while (events.hasNext) {
      val evt: AnyEvt.Reader = events.next
      handleEvent(evt, i)
      i += 1
    }
    println("loading "+(i-1)+" events took "+stopwatch.stop())
    getLastObjUsages()
  }

  def connect(overwrite: Boolean = false): Unit = {
    val cluster: Cluster =
      Cluster.builder()
        .addContactPoint("127.0.0.1")
        .build()


    if (overwrite) {
      initKeyspace(cluster, this.keyspace)
    }

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

    session.execute("CREATE TABLE "+keyspace+".objects(" +
      "id bigint, " +
      "klass text, " +
      "allocationSiteFile text, " +
      "allocationSiteLine bigint, " +
      "firstUsage bigint, " +
      "lastUsage bigint, " +
      "comment text, " +
      "PRIMARY KEY(id)) " +
      "WITH COMPRESSION = {};")

    session.execute("CREATE TABLE "+keyspace+".refs(" +
      "caller bigint, callee bigint, " +
      "kind text, "+
      "start bigint, end bigint, " +
      "comment text, " +
      "PRIMARY KEY(caller, callee, kind)) " +
      "WITH COMPRESSION = {};")

    session.execute("CREATE TABLE "+keyspace+".uses(" +
      "caller bigint, callee bigint, kind text, idx bigint, comment text, " +
      "PRIMARY KEY(caller, callee, idx)" +
      ") " +
      "WITH COMPRESSION = {};")

    session.close()
    this.session = cluster.connect(keyspace)

    this.insertObjectStatement = this.session.prepare("INSERT INTO objects(id, klass, allocationSiteFile, allocationSiteLine, comment) VALUES(?, ?, ?, ?, ?);")
    this.insertEdgeStatement   = this.session.prepare("INSERT INTO "+this.keyspace+".refs(caller, callee, kind, start, end, comment) VALUES(?, ?, ?, ?, ?, ?);")
    this.insertUseStatement    = this.session.prepare("INSERT INTO "+this.keyspace+".uses(caller, callee, kind, idx, comment) VALUES(?, ?, ?, ?, ?);")
  }

}

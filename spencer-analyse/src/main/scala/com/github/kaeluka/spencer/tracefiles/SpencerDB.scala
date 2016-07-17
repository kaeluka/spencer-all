package com.github.kaeluka.spencer.tracefiles

import java.io.{File, FileInputStream}

import com.datastax.driver.core._
import com.datastax.spark.connector.CassandraRow
import com.github.kaeluka.spencer.{DBLoader, Events}
import com.github.kaeluka.spencer.Events.{AnyEvt, ReadModifyEvt}
import com.google.common.base.Stopwatch
import org.apache.spark.SparkContext
import com.datastax.spark.connector._

/**
  * Created by stebr742 on 2016-07-01.
  */
class SpencerDB(val keyspace: String) {
  var insertUseStatement : PreparedStatement = null;
  var insertEdgeStatement : PreparedStatement = null
  var insertObjectStatement : PreparedStatement = null
  var session : Session = null
  val saveOrigin = false


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
          if (menter.getName.toString == "<init>") {
            //            if (menter.getCallsiteline != -1) {
            //              println("inserting: " + menter.getCalleetag)
            //            }
            insertObject(menter.getCalleetag, menter.getCalleeclass.toString, menter.getCallsitefile.toString, menter.getCallsiteline, EventsUtil.messageToString(evt))
          }
        case AnyEvt.Which.METHODEXIT => () //???
        case AnyEvt.Which.OBJALLOC => () //???
        case AnyEvt.Which.OBJFREE => () //???
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

  def getLastObjUsages(): Unit = {
    val sc: SparkContext = DBLoader.startSpark()

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
      "PRIMARY KEY(id)) ;")

    session.execute("CREATE TABLE "+keyspace+".refs(" +
      "caller bigint, callee bigint, " +
      "kind text, "+
      "start bigint, end bigint, " +
      "comment text, " +
      "PRIMARY KEY(caller, callee, kind));")

    session.execute("CREATE TABLE "+keyspace+".uses(" +
      "caller bigint, callee bigint, kind text, idx bigint, comment text, " +
      "PRIMARY KEY(caller, callee, idx)" +
      ");")

    session.close()
    this.session = cluster.connect(keyspace)

    this.insertObjectStatement = this.session.prepare("INSERT INTO objects(id, klass, allocationSiteFile, allocationSiteLine, comment) VALUES(?, ?, ?, ?, ?);")
    this.insertEdgeStatement   = this.session.prepare("INSERT INTO "+this.keyspace+".refs(caller, callee, kind, start, end, comment) VALUES(?, ?, ?, ?, ?, ?);")
    this.insertUseStatement    = this.session.prepare("INSERT INTO "+this.keyspace+".uses(caller, callee, kind, idx, comment) VALUES(?, ?, ?, ?, ?);")
  }

}

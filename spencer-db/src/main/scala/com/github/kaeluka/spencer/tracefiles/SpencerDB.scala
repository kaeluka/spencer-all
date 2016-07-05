package com.github.kaeluka.spencer.tracefiles

import java.io.{File, FileInputStream}
import java.util

import com.datastax.driver.core._
import com.github.kaeluka.spencer.Events
import com.github.kaeluka.spencer.Events.MethodEnterEvt.Reader
import com.github.kaeluka.spencer.Events.{AnyEvt, MethodEnterEvt, VarLoadEvt}
import com.google.common.base.Stopwatch
import org.apache.cassandra
import com.github.kaeluka.spencer.tracefiles.EventsUtil

/**
  * Created by stebr742 on 2016-07-01.
  */
class SpencerDB(val keyspace: String) {
  var insertEdgeStatement : PreparedStatement = null
  var insertObjectStatement : PreparedStatement = null

  def handleEvent(session: Session, evt: AnyEvt.Reader, idx: Int) {
    evt.which() match {
      case AnyEvt.Which.FIELDLOAD => {
        //        val fload = evt.getFieldload
        //        insertEdge(session, fload.getCallertag, fload.getHoldertag, "field load", idx, idx+1)
      }
      case AnyEvt.Which.FIELDSTORE => {
        val fstore = evt.getFieldstore
        insertEdge(session, fstore.getCallertag, fstore.getHoldertag, "field", idx)
      }
      case AnyEvt.Which.METHODENTER => {
        val menter = evt.getMethodenter
        if (menter.getName.toString == "<init>") {
          insertObject(session, menter.getCalleetag, menter.getCalleeclass.toString)
        }
        //        val menter: Reader = evt.getMethodenter
        //        val callerTag: Long = -1
        //        val end: Long = -1
        //        insertEdge(session, callerTag, menter.getCalleetag, "call", idx, end)
      }
      case AnyEvt.Which.METHODEXIT => ()//???
      case AnyEvt.Which.OBJALLOC => ()//???
      case AnyEvt.Which.OBJFREE => ()//???
      case AnyEvt.Which.READMODIFY => {
        //        val readmodify = evt.getReadmodify
        //        insertEdge(session, readmodify.getCallertag, readmodify.getCalleetag,
        //          if (readmodify.getIsModify) { "modify" } else { "read" }, idx, idx+1)
      }
      case AnyEvt.Which.VARLOAD => {
        //        val varload: Events.VarLoadEvt.Reader = evt.getVarload
        //        insertEdge(session, varload.getCallertag, varload.getVal, "varload", idx, idx+1)
      }
      case AnyEvt.Which.VARSTORE => {
        val varstore: Events.VarLoadEvt.Reader = evt.getVarload
        insertEdge(session, varstore.getCallertag, varstore.getVal, "var", idx)
      }
      case other =>
        throw new IllegalStateException(
          "do not know how to handle event kind "+other)
    }
  }

  def insertObject(session: Session, tag: Long, klass: String, comment: String = "none") {
    if (session.execute("SELECT id FROM "+this.keyspace+".objects WHERE id = "+tag).isExhausted) {
      //      session.executeAsync("INSERT INTO objects(id, klass, comment) VALUES("
      //        + tag + ", '"
      //        + klass + "', '"
      //        + comment + "');")
      session.executeAsync(this.insertObjectStatement.bind(
        tag : java.lang.Long,
        klass,
        comment))
    }
  }

  def insertEdge(session: Session, caller: Long, callee: Long, kind: String, start: Long, comment: String = "none") {

    session.executeAsync(this.insertEdgeStatement.bind(
      caller : java.lang.Long,
      callee : java.lang.Long,
      kind,
      start  : java.lang.Long,
      new java.lang.Long(-1),
      comment))
  }

  def loadFrom(f : File) {

    val stopwatch: Stopwatch = Stopwatch.createStarted

    val cluster : Cluster =
      Cluster.builder()
        .addContactPoint("127.0.0.1")
        .build()

    initKeyspace(cluster, this.keyspace)

    val session = connectToKeyspace(cluster, this.keyspace)

    val events: TraceFileIterator = new TraceFileIterator(new FileInputStream(f).getChannel)
    var i = 1
    events.foreach(
      evt => {
        handleEvent(session, evt, i)
        i += 1
      }
    )
    println("loading "+(i-1)+" events took "+stopwatch.stop())

    //    printObjectsTable(session)
  }

  //  def printObjectsTable(session: Session): Unit = {
  //    println("################ querying data....")
  //    val res: ResultSet = session.execute("SELECT * FROM spencerTest.objects;")
  //    val iter = res.iterator()
  //    while (!res.isFullyFetched) {
  //      res.fetchMoreResults()
  //      println("############### found: " + iter.next());
  //    }
  //  }

  def connectToKeyspace(cluster: Cluster, keyspace: String): Session = {
    cluster.connect(keyspace)
  }

  def initKeyspace(cluster: Cluster, keyspace: String): Session = {
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
      "comment text, " +
      "PRIMARY KEY(id))")
    session.execute("CREATE TABLE "+keyspace+".refs(" +
      "caller bigint, callee bigint, " +
      "kind text, "+
      "start bigint, end bigint, " +
      "comment text, " +
      "PRIMARY KEY(caller, callee, kind))")

    session.execute("CREATE TABLE "+keyspace+".uses(" +
      "caller bigint, callee bigint, idx bigint, kind text, " +
      "PRIMARY KEY(caller)" +
      ");")

    session.close()
    val connection: Session = cluster.connect(keyspace)

    this.insertObjectStatement = connection.prepare("INSERT INTO objects(id, klass, comment) VALUES(?, ?, ?);")
    this.insertEdgeStatement = connection.prepare("INSERT INTO "+this.keyspace+".refs(caller, callee, kind, start, end, comment) VALUES(?, ?, ?, ?, ?, ?);")
    //    "INSERT INTO "+this.keyspace+".refs(caller, callee, kind, start, end, comment) VALUES("
    //    + caller + ", "
    //    + callee + ", '"
    //    + kind + "', "
    //    + start + ", "
    //    + -1 + ", '" // the edge's end will be defined later
    //    + comment + "');"
    connection
  }

}

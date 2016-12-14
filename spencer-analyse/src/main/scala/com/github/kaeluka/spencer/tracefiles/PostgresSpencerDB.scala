package com.github.kaeluka.spencer

import java.io._
import java.nio.file.{Path, Paths}
import java.sql.{Connection, DriverManager}
import java.util.EmptyStackException

import com.github.kaeluka.spencer.Events.{AnyEvt, LateInitEvt, ReadModifyEvt}
import com.github.kaeluka.spencer.analysis._
import com.github.kaeluka.spencer.tracefiles.{EventsUtil, TraceFiles}
import com.google.common.base.Stopwatch
import org.apache.commons.io.FileUtils
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

object PostgresSpencerDB {
  val conf = new SparkConf()
    .setAppName("spencer-analyse")
//    .set("spark.cassandra.connection.host", "127.0.0.1")
    .set("spark.network.timeout", "1000")
    .set("spark.executor.heartbeatInterval", "1000")
    .setMaster("local[8]")
    //      .set("spark.Postgres.connection.host", "130.238.10.30")
    //      .setMaster("spark://Stephans-MacBook-Pro.local:7077")
    //              .set("spark.executor.memory", "4g").set("worker_max_heap", "1g")

  val sc : SparkContext = new SparkContext(conf)

  def shutdown() = {
    PostgresSpencerDB.sc.stop()
  }
}

class PostgresSpencerDB(dbname: String) extends SpencerDB {
  private var conn : Connection = _

  val sqlContext: SQLContext = new SQLContext(PostgresSpencerDB.sc)
  sqlContext.setConf("keyspace", dbname)

  override def shutdown() = {
  }

  var insertUseStatement : java.sql.PreparedStatement = _
  var insertUseBatchSize = 0;
  var insertEdgeStatement : java.sql.PreparedStatement = _
  var insertEdgeOpenStatement : java.sql.PreparedStatement = _
  var finishEdgeStatement : java.sql.PreparedStatement = _
  var insertCallStatement : java.sql.PreparedStatement = _
  var insertObjectStatement : java.sql.PreparedStatement = _
  val saveOrigin = dbname.equals("test")

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
                val Nvars = variables.length
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
        /*
        */
        case other => ()
//          throw new IllegalStateException(
//            "do not know how to handle event kind " + other)
      }
    } catch {
      case e: IllegalStateException =>
        ()
    }
  }

  def insertCall(caller: Long, callee: Long, name: String, start : Long, end: Long, thread: String, callSiteFile: String, callSiteLine: Long, comment: String = "none") {
    this.insertCallStatement.clearParameters()
    this.insertCallStatement.setLong  (1, caller)
    this.insertCallStatement.setLong  (2, callee)
    this.insertCallStatement.setString(3, name)
    this.insertCallStatement.setLong  (4, start)
    this.insertCallStatement.setLong  (5, end)
    this.insertCallStatement.setString(6, thread)
    this.insertCallStatement.setString(7, callSiteFile)
    this.insertCallStatement.setLong  (8, callSiteLine)
    this.insertCallStatement.setString(9, comment)
    this.insertCallStatement.execute()
//  "INSERT INTO ${this.dbname}.calls(caller, callee, name, start, end, thread, callsitefile, callsiteline, comment) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?);")
//    session.executeAsync(this.insertCallStatement.bind(
//      caller : java.lang.Long, callee : java.lang.Long,
//      name,
//      start : java.lang.Long, end : java.lang.Long,
//      thread,
//      callSiteFile, callSiteLine : java.lang.Long,
//      comment))
  }

  def insertObject(tag: Long, klass: String, allocationsitefile: String, allocationsiteline: Long, thread: String, comment: String = "none") {
    assert(allocationsitefile != null)
    assert(thread != null)
    assert(tag != 0)
    this.insertObjectStatement.clearParameters()
    this.insertObjectStatement.setLong  (1, tag)
    this.insertObjectStatement.setString(2, klass.replace('/', '.'))
    this.insertObjectStatement.setString(3, allocationsitefile)
    this.insertObjectStatement.setLong  (4, allocationsiteline)
    this.insertObjectStatement.setString(5, thread)
    if (saveOrigin) {
      this.insertObjectStatement.setString(6, comment)
    } else {
      this.insertObjectStatement.setString(6, "")
    }
    this.insertObjectStatement.execute()
  }

  def openEdge(holder: Long, callee: Long, kind: String, name: String, thread: String, start: Long, comment: String = "none") {
    assert(callee != 0, s"callee must not be 0: $comment")
    assert(holder != 0, s"holder must not be 0: $comment")
    assert(kind != null && kind.equals("var") || kind.equals("field"), "kind must be 'var' or 'field'")

    this.insertEdgeStatement.clearParameters()
    this.insertEdgeStatement.setLong  (1, holder)
    this.insertEdgeStatement.setLong  (2, callee)
    this.insertEdgeStatement.setString(3, kind)
    this.insertEdgeStatement.setString(4, name)
    this.insertEdgeStatement.setString(5, thread)
    this.insertEdgeStatement.setLong  (6, start)
    this.insertEdgeStatement.setString(7, if (saveOrigin) comment else "")
    this.insertEdgeStatement.execute()
  }

  def closeEdge(holder: Long, kind: String, start: Long, end: Long) {
    assert(holder != 0, "must have non-zero caller")
    assert(kind != null   && kind.equals("var") || kind.equals("field"), s"kind must be 'var' or 'field', but is $kind")
    this.finishEdgeStatement.clearParameters()
    this.finishEdgeStatement.setLong  (1, end)
    this.finishEdgeStatement.setString(2, kind)
    this.finishEdgeStatement.setLong  (3, start)
    this.finishEdgeStatement.setLong  (4, holder)
    this.finishEdgeStatement.execute()
  }

  def insertUse(caller: Long, callee: Long, method: String, kind: String, name: String, idx: Long, thread: String, comment: String = "none") {
    assert(caller != 0, s"caller must not be 0: $comment")
    assert(callee != 0, s"callee must not be 0: $comment")
    assert(method != null && method.length > 0, "method name must be given")
    assert(kind != null   && kind.equals("fieldstore") || kind.equals("fieldload") ||kind.equals("varload") ||  kind.equals("varstore") || kind.equals("read") || kind.equals("modify"), s"kind must be 'varstore/load' or 'fieldstore/load' or 'read' or 'modify', but is $kind")
    assert(idx > 0)
    assert(thread != null && thread.length > 0, "thread name must be given")

    this.insertUseStatement.clearParameters()
    this.insertUseStatement.setLong  (1, caller)
    this.insertUseStatement.setLong  (2, callee)
    this.insertUseStatement.setString(3, method)
    this.insertUseStatement.setString(4, kind)
    this.insertUseStatement.setString(5, name)
    this.insertUseStatement.setLong  (6, idx)
    this.insertUseStatement.setString(7, thread)
    this.insertUseStatement.setString(8, if (saveOrigin) comment else "")
    this.insertUseStatement.addBatch()
    this.insertUseBatchSize+=1
    if (insertUseBatchSize > 10000) {
      println("executing batch")
      this.insertUseStatement.executeBatch()
      println("executing batch: done")
      this.insertUseStatement.clearBatch()
      this.insertUseBatchSize = 0
    }
  }

  override def getCachedOrDo(name: String, f: () => DataFrame): DataFrame = {
    val opts = Map(
      "url" -> s"jdbc:postgresql:$dbname",
      "dbtable" -> name
    )
    var ret : DataFrame = null;
    try {
      println("trying to get frame...")
      ret = getFrame(name)
      println(s"found cached frame for $name")
    } catch {
      case e:Throwable => {
        println(s"didn't find cached frame for $name (${e.getMessage}")
        ret = f()
        println(s"caching: $name")
        assert(ret != null, "need result!")
        assert(ret.write != null )
        assert(ret.write.mode(SaveMode.Ignore) != null )
        ret.write.mode(SaveMode.Ignore).jdbc(s"jdbc:postgresql:$dbname", name, new java.util.Properties())
      }
    }
    ret
  }

  def aproposObject(tag: Long): AproposData = {
//    ???
    val objTable = getFrame("objects").rdd

    val theObj = objTable
      .filter(_.getAs[Long]("id") == tag)

    val klass = Option(theObj
      .first()
      .getAs[String]("klass"))

    var allocMsg  = ""
    if (theObj.count == 0) {
      allocMsg += "<not initialised>\n"
    } else {
      allocMsg += theObj.collect()(0)+"\n"
      allocMsg += "allocated at:\n  - "
      allocMsg += theObj
        .map(row => Option(row.getAs[String]("allocationsitefile")).toString +":"+Option(row.getAs[Long]("allocationsiteline")).toString)
        .collect.mkString("\n  - ")
      allocMsg += "\n"
    }

    val usesTable = this.selectFrame("uses", s"SELECT * FROM uses WHERE caller = $tag OR callee = $tag").rdd
    val uses = usesTable
        .map(row=>
          (row.getAs[Long]("caller"),
            row.getAs[Long]("callee"),
            row.getAs[Long]("idx"),
            Option(row.getAs[String]("kind")).getOrElse("<unknown kind>"),
            Option(row.getAs[String]("name")).getOrElse("<unknown name>"),
            Option(row.getAs[String]("thread")).getOrElse("<unknown thread>"),
            row.getAs[String]("comment"))
        )

    val useEvents = uses.map {
      case ((caller, callee, idx, kind, name, thread, comment)) => AproposUseEvent(caller, callee, idx, kind, name, thread, "use "+comment).asInstanceOf[AproposEvent]
    }

    val callsTable = this.selectFrame("calls", s"SELECT * FROM calls WHERE caller = $tag OR callee = $tag").rdd
    val callsEvents = callsTable
        .map(row =>
          AproposCallEvent(row.getAs[Long]("caller")
            , row.getAs[Long]("callee")
            , row.getAs[Long]("callstart")
            , row.getAs[Long]("callend")
            , row.getAs[String]("name")
            , row.getAs[String]("callsitefile") + ":" + row.getAs[Long]("callsiteline")
            , Option(row.getAs[String]("thread")).getOrElse("<unknown thread>")
            , row.getAs[String]("comment")).asInstanceOf[AproposEvent]
        )

    val refsTable = this.selectFrame("refs", s"SELECT * FROM refs WHERE caller = $tag OR callee = $tag").rdd
    val refsEvents =
      refsTable.map(row =>
        AproposRefEvent(
          row.getAs[Long]("caller")
          , row.getAs[Long]("callee")
          , row.getAs[Long]("refstart")
          , Option(row.getAs[Long]("refend"))
          , row.getAs[String]("name")
          , row.getAs[String]("kind")
          , Option(row.getAs[String]("thread")).getOrElse("<unknown thread>")
          , row.getAs[String]("comment")).asInstanceOf[AproposEvent])

    AproposData(
      None,
      (useEvents++callsEvents++refsEvents)
        .sortBy(AproposEvent.startTime).distinct().collect(), klass)
  }

//  override def selectFrame(query: String) : DataFrame = {
//    this.sqlContext.sql(query.replace("FROM ", s"FROM ${this.dbname}."))
//  }

  def getFrame(table: String): DataFrame = {
    val opts = Map(
      "url" -> s"jdbc:postgresql:$dbname",
      "dbtable" -> table
    )

    this.sqlContext.read.format("jdbc").options(opts).load()
  }

  private def initGraph: Graph[ObjDesc, EdgeDesc] = {
    import sqlContext.implicits._
    val objs = this.getFrame("objects").select("id", "klass").as[(Long, String)].map {
      case (id, k) => (id, ObjDesc(klass = Option(k)))
    }

    //    val uses = this.db.getTable("uses")
    //      .map(row => {
    //        val fr = row.getLong("idx")
    //        val to = fr + 1
    //        Edge(
    //          row.getLong("caller"),
    //          row.getLong("callee"),
    //          EdgeDesc(Some(fr), Some(to), EdgeKind.fromUsesKind(row.getString("kind")))
    //        )
    //      })
    //      .setName("object graph edges")
    val refs = this.getFrame("refs").select("caller", "callee", "refstart", "refend", "kind")
      .rdd
      .map(row => {
        val fr = Option(row.getAs[Long]("refstart"))
        val to = Option(row.getAs[Long]("refend"))
        Edge(
          row.getAs[Long]("caller"),
          row.getAs[Long]("callee"),
          EdgeDesc(fr, to, EdgeKind.fromRefsKind(row.getAs[String]("kind"))))
      })

    val g: Graph[ObjDesc, EdgeDesc] =
      Graph(objs.rdd, refs)
    g.cache()
    g
  }

  private lazy val g = initGraph


  def getGraph(): Graph[ObjDesc, EdgeDesc] = {
    this.g
  }

  override def clearCaches(dbname: Option[String]): Unit = {
    ???
  }

//  @deprecated
//  def getTable(table: String): CassandraTableScanRDD[CassandraRow] = {
//    assert(PostgresSpencerDB.sc != null, "need to have spark context")
//    assert(this.session != null, "need to have db session")
//    val ret = PostgresSpencerDB.sc.cassandraTable(this.session.getLoggedKeyspace, table)
//    ret
//  }

//  def getProperObjects: RDD[CassandraRow] = {
//    //FIXME: use WHERE clause
//    getTable("objects")
//      .filter(_.getLong("id") > 0)
//      .filter(
//        _.getStringOption("comment")
//          .forall(!_.contains("late initialisation")))
//  }

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

    val my_calls= getFrame("calls")
    my_calls.createOrReplaceTempView("my_calls")

    val selection = this.sqlContext.sql(
      """SELECT
        |  *
        |FROM  my_calls
        |WHERE name = '<init>'""".stripMargin)

//    selection.show(10)

    val rdd = selection.rdd

    val correctionMap = rdd
      .groupBy(_.getAs[Long]("callee"))
      .filter(_._2.size >= 2)
      .map {
        case (callee, _calls) => {
          val calls = _calls.toArray
          val times = calls.flatMap(call => List(
            call.getAs[Long]("callstart"),
            call.getAs[Long]("callend"))).sorted
          calls.sortBy(call => -1 * call.getAs[Long]("callstart"))
          (callee, calls.zipWithIndex.map {
            case (call, idx) =>
              (
                call,
                (call.getAs[Long]("callstart"), call.getAs[Long]("callend")) -> (times(idx), times(times.length - idx - 1)))
          })
        }
      }.sortBy(x => x._1)
      .collect()

    correctionMap.foreach {
      case (callee, corrections) =>
        corrections.foreach {
          case (call, ((origStart, origEnd), (newStart, newEnd))) =>
            //FIXME: the caller must be THIS, except for the first call
            this.conn.createStatement().execute(s"UPDATE calls SET callstart = $newStart, callend = $newEnd WHERE callstart = $origStart AND callend = $origEnd")
        }
    }
  }

  def computeEdgeEnds(): Unit = {

    val my_refs = this.getFrame("refs")
    my_refs.createOrReplaceTempView("my_refs")

    val df = this.sqlContext.sql(
      """SELECT *
        |FROM my_refs
        |WHERE kind = 'field'""".stripMargin)

    df.show(10)

    val rdd = df.rdd

    val groupedAssignments = rdd
      .groupBy(row => (row.getAs[Long]("caller"), row.getAs[String]("name")))
      .map({case ((caller, name), rows) =>
        val sorted =
          rows.toSeq.sortBy(_.getAs[Long]("refstart"))
            .map(row =>
              (row.getAs[Long]("refstart"), row.getAs[String]("kind"))
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
    this.sqlContext.sql("SELECT * FROM my_refs").show(10)
  }

  def computeLastObjUsages(): Unit = {

    //FIXME: this could use the dataframe API for better performance
    //the first action involving an object is always its constructor call
    val firstLastCalls = this.getFrame("calls")
      .select("callee", "callstart", "callend")
      .rdd
      .groupBy(_.getAs[Long]("callee"))
      .map {
        case (id, calls) =>
          (id, (calls.map(_.getAs[Long]("callstart")).min, calls.map(_.getAs[Long]("callend")).max))
      }

    //the last action involving an object is always a usage
    val firstLastUsages = this.getFrame("uses").select("idx", "callee").rdd
      .groupBy(_.getAs[Long]("callee"))
      .map(
        {
          case (id, rows) =>
            (id, (rows.map(_.getAs[Long]("idx")).min, rows.map(_.getAs[Long]("idx")).max))
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
          this.conn.createStatement().execute(
            s"""UPDATE objects
              |SET
              |  firstUsage = $fst,
              |  lastUsage  = $lst
              |WHERE id = $id""".stripMargin)
//          this.session.execute(
//            "  UPDATE objects " +
//              "SET " +
//              s"  firstUsage = ${fst}, " +
//              s"  lastUsage  = ${lst}" +
//              "WHERE id = " + id)
      }
    }
  }

//  def generatePerClassObjectsTable(): Unit = {
//    getFrame("objects").foreach(row => {
//      val id = row.getAs[Long]("id").asInstanceOf[java.lang.Long]
//      val klass = Option(row.getAs[String]("klass"))
//      val allocationsitefile = row.getAs[String]("allocationsitefile")
//      val allocationsiteline = row.getAs[Long]("allocationsiteline")
//      val firstUsage = row.getLong("firstusage").asInstanceOf[java.lang.Long]
//      val lastUsage = row.getLong("lastusage").asInstanceOf[java.lang.Long]
//      val thread = row.getStringOption("thread").getOrElse(null)
//      val comment = row.getStringOption("comment").getOrElse(null)
//
//      this.session.execute("INSERT INTO objects_per_class (" +
//        "id, klass, allocationsitefile, allocationsiteline, firstusage, " +
//        "lastusage, thread, comment) VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
//        id, klass.getOrElse("<unknown class>"), allocationsitefile, allocationsiteline, firstUsage, lastUsage,
//        thread, comment)
//    })
//  }

  def storeClassFile(logDir: Path, file: File) {
    val relPath = logDir.relativize(file.toPath)

    val segments = for (i <- 0 until relPath.getNameCount) yield relPath.getName(i)
    val className = segments.mkString(".").replace(".class", "")
    print(s"storing class $className...")
    val fileStream = new FileInputStream(file)

    val stat = this.conn.prepareStatement("INSERT INTO classdumps VALUES (?, ?)")
    stat.setString(1, className)
    stat.setBinaryStream(2, fileStream)
    stat.execute()
    println("done")
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

    this.connect_(true)

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

    if (this.insertUseBatchSize > 0) {
      this.insertUseStatement.executeBatch()
    }
    sortConstructorCalls()
    computeEdgeEnds()
    computeLastObjUsages()
    /*
    generatePerClassObjectsTable()
    */
    this.shutdown()
  }

  def connect(): Unit = {
    this.connect_(false)
  }

  def connect_(overwrite: Boolean = false): Unit = {

    if (overwrite) {
      createFreshTables(this.dbname)
    }

    initPreparedStatements(this.dbname)

//    connectToKeyspace(this.dbname)
  }

  @deprecated
  def connectToKeyspace(keyspace: String): Unit = {
  }

  def initDbConnection(): Unit = {

    val opts = Map(
      "url"     -> s"jdbc:postgresql:$dbname",
      "dbtable" -> dbname,
      "user"    -> "spencer"
    )
    if (this.conn != null) {
      this.conn.close()
    }
    this.conn = DriverManager.getConnection(s"jdbc:postgresql:$dbname")
    this.conn.setAutoCommit(false)
  }

  def createFreshTables(dbname: String) {
    if (this.conn != null) {
      this.conn.close()
    }
    this.conn = DriverManager.getConnection("jdbc:postgresql:template1")
    this.conn.createStatement().execute(s"DROP DATABASE IF EXISTS $dbname")

    this.conn.createStatement().execute(s"CREATE DATABASE $dbname")
    initDbConnection()
    this.conn.createStatement().execute(
      """CREATE TABLE objects (
        |  id bigint,
        |  klass text,
        |  allocationsitefile text,
        |  allocationsiteline integer,
        |  firstUsage bigint,
        |  lastUsage bigint,
        |  thread text,
        |  comment text,
        |  PRIMARY KEY(id))""".stripMargin)

    this.conn.createStatement().execute(
      """CREATE TABLE refs (
        |  caller bigint,
        |  callee bigint,
        |  kind varchar(10),
        |  name varchar(80),
        |  refstart bigint,
        |  refend bigint,
        |  thread text,
        |  comment text,
        |  PRIMARY KEY (caller, kind, refstart))
       """.stripMargin)
    this.conn.createStatement().execute(
      """CREATE TABLE calls (
        |  caller bigint,
        |  callee bigint,
        |  name text,
        |  callstart bigint,
        |  callend bigint,
        |  callsitefile text,
        |  callsiteline bigint,
        |  thread text,
        |  comment text,
        |  PRIMARY KEY (caller, callee, callstart, callend))
      """.stripMargin)
    this.conn.createStatement().execute(
      """CREATE TABLE uses (
        |  caller bigint,
        |  callee bigint,
        |  name text,
        |  method text,
        |  kind text,
        |  idx bigint,
        |  thread varchar(80),
        |  comment text,
        |  PRIMARY KEY(caller, callee, idx))
      """.stripMargin)

    this.conn.createStatement().execute(
      """CREATE TABLE classdumps (
        |  classname text,
        |  bytecode bytea,
        |  PRIMARY KEY(classname))
      """.stripMargin)
    this.conn.commit()
  }

  def initPreparedStatements(keyspace: String): Unit = {
    this.initDbConnection()

    this.insertObjectStatement = this.conn.prepareStatement(
      """INSERT INTO objects (
        |  id,
        |  klass,
        |  allocationsitefile,
        |  allocationsiteline,
        |  thread,
        |  comment) VALUES (?, ?, ?, ?, ?, ?)
        |  ON CONFLICT DO NOTHING""".stripMargin)
    this.insertEdgeStatement = this.conn.prepareStatement(
      """INSERT INTO refs (
        |  caller,
        |  callee,
        |  kind,
        |  name,
        |  thread,
        |  refstart,
        |  comment) VALUES (?, ?, ?, ?, ?, ?, ? )""".stripMargin)
    this.finishEdgeStatement = this.conn.prepareStatement(
      """UPDATE refs
        |SET refend = ?
        |WHERE kind = ? AND refstart = ? and caller = ?""".stripMargin)

    this.insertUseStatement = this.conn.prepareStatement(
      """INSERT INTO uses (
        |  caller,
        |  callee,
        |  method,
        |  kind,
        |  name,
        |  idx,
        |  thread,
        |  comment) VALUES ( ?, ?, ?, ?, ?, ?, ?, ? )""".stripMargin)
    this.insertCallStatement = this.conn.prepareStatement(
      """INSERT INTO calls (
        |  caller,
        |  callee,
        |  name,
        |  callstart,
        |  callend,
        |  thread,
        |  callsitefile,
        |  callsiteline,
        |  comment) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin)
  }
}

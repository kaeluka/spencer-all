package com.github.kaeluka.spencer

import java.io._
import java.nio.file.{Path, Paths}
import java.sql.{Connection, DriverManager, ResultSet}
import java.util.concurrent.TimeUnit
import java.util.{Calendar, EmptyStackException}

import com.github.kaeluka.spencer.Events.{AnyEvt, LateInitEvt, ReadModifyEvt}
import com.github.kaeluka.spencer.analysis._
import com.github.kaeluka.spencer.tracefiles.{EventsUtil, TraceFiles}
import com.google.common.base.Stopwatch
import org.apache.commons.io.FileUtils
import org.postgresql.util.PSQLException

import scala.collection.JavaConversions._

object PostgresSpencerDBs {
  def getAvailableBenchmarks(): Seq[BenchmarkMetaInfo] = {
    val conn = DriverManager.getConnection("jdbc:postgresql://postgres/template1", System.getenv("POSTGRES_USER"), System.getenv("POSTGRES_PASSWORD"))
    var benchmarks = List[BenchmarkMetaInfo]()
    val ps = conn.prepareStatement("SELECT datname FROM pg_database WHERE datistemplate = false;")
    val rs = ps.executeQuery()
    var db: PostgresSpencerDB = null
    while (rs.next()) {
      val dbname = rs.getString(1)
      try {
        val dbconn = DriverManager.getConnection(s"jdbc:postgresql://postgres/$dbname", System.getenv("POSTGRES_USER"), System.getenv("POSTGRES_PASSWORD"))

        db = new PostgresSpencerDB(dbname)
        db.connect()

        val countRes = db.conn.createStatement().executeQuery("SELECT COUNT(id) FROM objects")
        assert(countRes.next())
        val count = countRes.getLong(1)
        countRes.close()

        val dateRes = db.conn.createStatement().executeQuery("SELECT val FROM meta WHERE key = 'date'")
        val date = if (dateRes.next()) {
          dateRes.getString(1)
        } else {
          null
        }
        dateRes.close()

        val commentRes = db.conn.createStatement().executeQuery("SELECT val FROM meta WHERE key = 'comment'")
        val comment = if (commentRes.next()) {
          commentRes.getString(1)
        } else {
          null
        }
        commentRes.close()

        benchmarks = benchmarks ++ List(BenchmarkMetaInfo(dbname, count, date, comment))
      } catch {
        case e: PSQLException => ()
      } finally {
        db.shutdown()
      }
    }
    rs.close()
    ps.close()
    conn.close()
    benchmarks
  }
}

class PostgresSpencerDB(dbname: String) {
  var conn : Connection = _

  def shutdown() = {
    this.conn.close()
  }

  var insertUseStatement : java.sql.PreparedStatement = _
  var insertUseBatchSize = 0

  var insertEdgeStatement : java.sql.PreparedStatement = _
  var insertEdgeBatchSize = 0

  var finishEdgeStatement : java.sql.PreparedStatement = _
  var finishEdgeBatchSize = 0

  var insertCallStatement : java.sql.PreparedStatement = _
  var insertCallBatchSize = 0

  var insertObjectStatement : java.sql.PreparedStatement = _

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
            fieldload.getThreadName.toString)
        case AnyEvt.Which.FIELDSTORE =>
          val fstore = evt.getFieldstore
          insertUse(
            caller  = fstore.getCallertag,
            callee  = fstore.getHoldertag,
            method  = fstore.getCallermethod.toString,
            kind    = "fieldstore",
            name    = fstore.getFname.toString,
            idx     = idx,
            thread  = fstore.getThreadName.toString)
          if (fstore.getNewval != 0) {
            openEdge(
              holder  = fstore.getHoldertag,
              callee  = fstore.getNewval,
              kind    = "field",
              name    = fstore.getFname.toString,
              thread  = fstore.getThreadName.toString,
              start   = idx)
          }
        case AnyEvt.Which.METHODENTER =>
          val menter = evt.getMethodenter
          stacks.push(menter, idx)

          if (menter.getName.toString == "<init>") {
            insertObject(menter.getCalleetag, menter.getCalleeclass.toString,
              menter.getCallsitefile.toString, menter.getCallsiteline,
              menter.getThreadName.toString)
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
                  callSiteLine = menter.enter.getCallsiteline)
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
              openEdge(lateinit.getCalleetag, fld.getVal, "field", fld.getName.toString, "<JVM thread>", 1)
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
            thread = readmodify.getThreadName.toString)
        case AnyEvt.Which.VARLOAD =>
          val varload: Events.VarLoadEvt.Reader = evt.getVarload
          insertUse(
            caller = varload.getCallertag,
            callee = varload.getCallertag,
            method = varload.getCallermethod.toString,
            kind = "varload",
            name ="var_"+varload.getVar.toString,
            idx = idx,
            thread = varload.getThreadName.toString)
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
            thread = varstore.getThreadName.toString)
          if (! stacks.peek(varstore.getThreadName.toString).map(_.enter.getCalleetag).contains(varstore.getCallertag) ) {
            println(s"""at $idx: last enter's callee tag and varstore's caller tag do not match:
                        |enter   : ${stacks.peek(varstore.getThreadName.toString)}
                        |varstore: ${EventsUtil.varStoreToString(varstore)}""".stripMargin)
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
              start = idx)
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

  def insertCall(caller: Long, callee: Long, name: String, start : Long, end: Long, thread: String, callSiteFile: String, callSiteLine: Long) {
    this.insertCallStatement.clearParameters()
    this.insertCallStatement.setLong  (1, caller)
    this.insertCallStatement.setLong  (2, callee)
    this.insertCallStatement.setString(3, name)
    this.insertCallStatement.setLong  (4, start)
    this.insertCallStatement.setLong  (5, end)
    this.insertCallStatement.setString(6, thread)
    this.insertCallStatement.setString(7, callSiteFile)
    this.insertCallStatement.setLong  (8, callSiteLine)
    this.insertCallStatement.addBatch()
    this.insertCallBatchSize += 1
    if (insertCallBatchSize > 10000) {
      this.insertCallStatement.executeBatch()
      this.conn.commit()
      this.insertCallStatement.clearBatch()
      this.insertCallBatchSize = 0
    }
  }

  def insertObject(tag: Long, klass: String, allocationsitefile: String, allocationsiteline: Long, thread: String) {
    assert(allocationsitefile != null)
    assert(thread != null)
    assert(tag != 0)
    this.insertObjectStatement.clearParameters()
    this.insertObjectStatement.setLong  (1, tag)
    this.insertObjectStatement.setString(2, klass.replace('/', '.'))
    this.insertObjectStatement.setString(3, allocationsitefile)
    this.insertObjectStatement.setLong  (4, allocationsiteline)
    this.insertObjectStatement.setString(5, thread)
    this.insertObjectStatement.execute()
  }

  def openEdge(holder: Long, callee: Long, kind: String, name: String, thread: String, start: Long) {
    assert(callee != 0, s"callee must not be 0")
    assert(holder != 0, s"holder must not be 0")
    assert(kind != null && kind.equals("var") || kind.equals("field"), "kind must be 'var' or 'field'")

    this.insertEdgeStatement.clearParameters()
    this.insertEdgeStatement.setLong  (1, holder)
    this.insertEdgeStatement.setLong  (2, callee)
    this.insertEdgeStatement.setString(3, kind)
    this.insertEdgeStatement.setString(4, name)
    this.insertEdgeStatement.setString(5, thread)
    this.insertEdgeStatement.setLong  (6, start)
    this.insertEdgeStatement.addBatch()
    this.insertEdgeBatchSize += 1
    if (insertEdgeBatchSize > 10000) {
      this.insertEdgeStatement.executeBatch()
      this.conn.commit()
      this.insertEdgeStatement.clearBatch()
      this.insertEdgeBatchSize = 0
    }
  }

  def closeEdge(holder: Long, kind: String, start: Long, end: Long) {
    assert(holder != 0, "must have non-zero caller")
    assert(kind != null   && kind.equals("var") || kind.equals("field"), s"kind must be 'var' or 'field', but is $kind")
    this.finishEdgeStatement.clearParameters()
    this.finishEdgeStatement.setLong  (1, end)
    this.finishEdgeStatement.setString(2, kind)
    this.finishEdgeStatement.setLong  (3, start)
    this.finishEdgeStatement.setLong  (4, holder)
    this.finishEdgeStatement.addBatch()
    this.finishEdgeBatchSize += 1
    if (finishEdgeBatchSize > 10000) {
      this.finishEdgeStatement.executeBatch()
      this.conn.commit()
      this.finishEdgeStatement.clearBatch()
      this.finishEdgeBatchSize = 0
    }
  }

  def insertUse(caller: Long, callee: Long, method: String, kind: String, name: String, idx: Long, thread: String) {
    assert(caller != 0, s"#$idx: caller must not be 0")
    assert(callee != 0, s"#$idx: callee must not be 0")
    assert(method != null && method.length > 0, "#$idx: method name must be given")
    assert(kind != null   && kind.equals("fieldstore") || kind.equals("fieldload") ||kind.equals("varload") ||  kind.equals("varstore") || kind.equals("read") || kind.equals("modify"), s"#$idx: kind must be 'varstore/load' or 'fieldstore/load' or 'read' or 'modify', but is $kind")
    assert(idx > 0)
    assert(thread != null && thread.length > 0, "#$idx: thread name must be given")

    this.insertUseStatement.clearParameters()
    this.insertUseStatement.setLong  (1, caller)
    this.insertUseStatement.setLong  (2, callee)
    this.insertUseStatement.setString(3, method)
    this.insertUseStatement.setString(4, kind)
    this.insertUseStatement.setString(5, name)
    this.insertUseStatement.setLong  (6, idx)
    this.insertUseStatement.setString(7, thread)
    this.insertUseStatement.addBatch()
    this.insertUseBatchSize+=1
    if (insertUseBatchSize > 10000) {
      this.insertUseStatement.executeBatch()
      this.conn.commit()
      this.insertUseStatement.clearBatch()
      this.insertUseBatchSize = 0
    }
  }

  def prepareCaches(sqls: Seq[String]): Unit = {
    sqls.foreach(sql => {
      this.conn.createStatement().execute(sql)
      this.conn.commit()
    })
  }

  /** Runs a query using caching.
    * The user has to close the ResultsSet
    *
    * @return the result set, stemming either from loading the cache,
    *         or from running the full query
    */
  def getCachedOrRunQuery(query: ResultSetAnalyser): ResultSet = {
    this.prepareCaches(query.precacheInnersSQL)
    this.getCachedOrRunSQL(query.cacheKey, query.getSQLUsingCache)
  }

  def runSQLQuery(sql: String): ResultSet = {
    try {
      this.conn.createStatement().executeQuery(sql)
    } catch {
      case e: PSQLException =>
        //add the original query to the exception for better debugability
        throw new PSQLException(s"query: $sql", null, e)
    }
  }

  /** Runs a SQL query using caching.
    * The user has to close the ResultsSet
    *
    * @return the result set, stemming either from loading the cache,
    *         or from running the full query
    */
  def getCachedOrRunSQL(cacheKey: String, sql: String): ResultSet = {
    assert(cacheKey != null)
    assert(sql != null)
    assert(this.conn != null)
    var ret: ResultSet = null
    try {
      ret = this.runSQLQuery(s"SELECT * FROM $cacheKey;")
    } catch {
      case e: PSQLException =>
        this.conn.commit()

        this.conn.createStatement().execute(
          s"CREATE TABLE IF NOT EXISTS $cacheKey AS $sql ;")
        this.conn.commit()
        ret = this.runSQLQuery(s"SELECT * FROM $cacheKey")
    }
    ret
  }

  def getObjPercentage(query: String): Option[Float] = {
    QueryParser.parseObjQuery(query) match {
      case Left(_err) => None
      case Right(q) =>
        this.prepareCaches(q.precacheInnersSQL)
        this.getCachedOrRunQuery(q).close()
        val result = this.getCachedOrRunSQL("cache_perc_"+s"getPercentages($query)".hashCode.toString.replace("-","_"),
          s"""SELECT
              |  ROUND(100.0*COUNT(id)/(SELECT COUNT(id) FROM objects WHERE id > 4), 2)
              |FROM
              |  (${q.getCacheSQL}) counted""".
            stripMargin)
        assert(result.next())
        val ret = result.getFloat(1)
        result.close()
        Some(ret)
    }
  }

  def getFieldPercentage(query: String, minInstances: Int = 10): Option[Seq[(String, Float)]] = {
    QueryParser.parseObjQuery(s"And(Obj() $query)") match {
      case Left(_err) => None
      case Right(q) =>
        val cacheKey = s"cache_fieldpercent_n${minInstances}_"+(s"getPercentages($q)".hashCode.toString.replace("-","_"))
        println(s"caching percentage of $query into $cacheKey")
        this.prepareCaches(q.precacheInnersSQL)
        this.getCachedOrRunQuery(q).close()
        val result = this.getCachedOrRunSQL(cacheKey,
          s"""-- select all fields, the number of objects, the number of passed objects, and the percentage
             |SELECT total.field field, total.ncallees total, COALESCE(passes.ncallees,0) passed, round(100.0*COALESCE(passes.ncallees,0)/total.ncallees, 2) perc FROM
             |(
             |-- select all fields, and the number of objects that were referenced from them
             |SELECT
             |  CONCAT(klass,'::',name) field, COUNT(DISTINCT callee) ncallees
             |FROM
             |  refs
             |INNER JOIN objects ON objects.id = refs.caller
             |WHERE kind = 'field'
             |AND   klass NOT LIKE '[%'
             |AND   callee IN (SELECT id FROM objects)
             |
             |GROUP BY (klass, name)
             |) total
             |LEFT OUTER JOIN
             |(
             |-- select all fields, and the number of objects that passed that were referenced
             |-- from them
             |SELECT
             |  CONCAT(klass,'::',name) field, COUNT(DISTINCT callee) ncallees
             |FROM
             |  refs
             |INNER JOIN objects ON objects.id = refs.caller
             |WHERE
             |  kind = 'field' AND
             |  klass NOT LIKE '[%'
             |  AND callee IN (${q.getCacheSQL}) -- <<<<< THIS IS THE INNER QUERY
             |GROUP BY (klass, name)
             |) passes
             |ON total.field = passes.field
             |ORDER BY perc DESC
           """.stripMargin)
        val ret = collection.mutable.ListBuffer[(String, Float)]()
        while (result.next) {
          ret.append((result.getString("field"), result.getFloat("perc")))
        }

        result.close()
        Some(ret)
    }
  }
  def getClassPercentage(query: String, minInstances: Int = 10): Option[Seq[(String, Float)]] = {
    QueryParser.parseObjQuery(s"And(Obj() $query)") match {
      case Left(_err) => None
      case Right(q) =>
        val cacheKey = s"cache_classpercent_n${minInstances}_"+(s"getPercentages($q)".hashCode.toString.replace("-","_"))
        println(s"caching percentage of $query into $cacheKey")
        this.prepareCaches(q.precacheInnersSQL)
        this.getCachedOrRunQuery(q).close()
        val result = this.getCachedOrRunSQL(cacheKey,
          s"""SELECT
             |  filtered.klass,
             |  ROUND(100.0*npassed/ntotal,2) AS percentage
             |FROM
             |((SELECT objects.klass, COUNT(objects.id) npassed
             |FROM   objects
             |JOIN
             |(
             |  ${q.getCacheSQL}
             |) AS counted
             |ON objects.id = counted.id
             |GROUP BY klass) filtered
             |RIGHT OUTER JOIN (SELECT klass, COUNT(id) ntotal
             |                  FROM objects
             |                  WHERE id > 4
             |                  GROUP BY klass) total
             |ON filtered.klass = total.klass)
             |WHERE ntotal >= $minInstances""".stripMargin)
        val ret = collection.mutable.ListBuffer[(String, Float)]()
        while (result.next) {
          ret.append((result.getString(1), result.getFloat(2)))
        }

        result.close()
        Some(ret)
    }
  }

  def aproposObject(tag: Long): AproposData = {
    val theObj = this.runSQLQuery(s"SELECT klass FROM objects WHERE id = $tag")

    val klass = if (theObj.next()) {
      Option(theObj.getString("klass"))
    } else {
      None
    }

    val usesTable = this.runSQLQuery(s"SELECT * FROM uses WHERE caller = $tag OR callee = $tag")
    val useEvents = collection.mutable.ArrayBuffer[AproposUseEvent]()
    while (usesTable.next) {
      useEvents +=
        AproposUseEvent(usesTable.getLong("caller"),
          usesTable.getLong("callee"),
          usesTable.getLong("idx"),
          Option(usesTable.getString("kind")).getOrElse("<unknown kind>"),
          Option(usesTable.getString("name")).getOrElse("<unknown name>"),
          Option(usesTable.getString("thread")).getOrElse("<unknown thread>"),
          "no comment")
    }
    usesTable.close()

    val callsTable = this.runSQLQuery(s"SELECT * FROM calls WHERE caller = $tag OR callee = $tag")
    val callsEvents = collection.mutable.ArrayBuffer[AproposCallEvent]()
    while (callsTable.next) {
      callsEvents +=
        AproposCallEvent(
          callsTable.getLong("caller")
          , callsTable.getLong("callee")
          , callsTable.getLong("callstart")
          , callsTable.getLong("callend")
          , callsTable.getString("name")
          , callsTable.getString("callsitefile") + ":" + callsTable.getLong("callsiteline")
          , Option(callsTable.getString("thread")).getOrElse("<unknown thread>")
          , "no comment")
    }
    callsTable.close()

    val refsTable = this.runSQLQuery(s"SELECT * FROM refs WHERE caller = $tag OR callee = $tag")
    val refsEvents = collection.mutable.ArrayBuffer[AproposRefEvent]()
    while (refsTable.next) {
      refsEvents +=
        AproposRefEvent(
          refsTable.getLong("caller")
          , refsTable.getLong("callee")
          , refsTable.getLong("refstart")
          , Option(refsTable.getLong("refend"))
          , refsTable.getString("name")
          , refsTable.getString("kind")
          , Option(refsTable.getString("thread")).getOrElse("<unknown thread>")
          , "no comment")
    }
    refsTable.close()

    var evts = collection.mutable.ArrayBuffer[AproposEvent]()
    evts.appendAll(useEvents)
    evts.appendAll(callsEvents)
    evts.appendAll(refsEvents)
    evts = evts.sortWith({ case (evt1, evt2) => AproposEvent.startTime(evt1) < AproposEvent.startTime(evt2) })

    AproposData(None, evts, klass)
  }

  def clearCaches(dbname: String, onlyStatistics: Boolean = false): Unit = {
    val conn = DriverManager.getConnection("jdbc:postgresql://postgres/"+dbname, System.getenv("POSTGRES_USER"), System.getenv("POSTGRES_PASSWORD"))
    val rs = conn.createStatement().executeQuery(
      s"""SELECT table_name
          |FROM information_schema.tables
          |WHERE table_schema='public'
          |AND   table_type='BASE TABLE'
          |AND   table_name LIKE 'cache_%${if (onlyStatistics) {"perc%"} else {""}}'""".stripMargin)
    println("\nclearing caches: ")
    while (rs.next()) {
      val tblname = rs.getString(1)
      if (tblname.startsWith("cache_")) {
        print(tblname+" ")
        this.conn.createStatement().execute(s"DROP TABLE IF EXISTS ${tblname}")
        this.conn.commit()
      }
    }
    rs.close()
    conn.close()
  }

//  def getProperObjects: RDD[CassandraRow] = {
//    //FIXME: use WHERE clause
//    getTable("objects")
//      .filter(_.getLong("id") > 0)
//      .filter(
//        _.getStringOption("comment")
//          .forall(!_.contains("late initialisation")))
//  }

  def createIndices(): Unit = {
    this.conn.createStatement().execute(
      "CREATE INDEX calls_callstart_idx ON calls(callstart)")

    this.conn.createStatement().execute(
      "CREATE INDEX calls_callend_idx ON calls(callend)")

    this.conn.createStatement().execute(
      "CREATE INDEX uses_name_idx ON uses(name)")

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
    * Then this method will simply update the start and end times of the first
    * call to span all constructor calls and delete the others
    * to be:
    *
    * #10 call(<init>,12) // A,B,C constructor
    * #60 exit(<init>)    // A,B,C constructor
    */
  def sortConstructorCalls(): Unit = {
    val watch = Stopwatch.createStarted()
    print("getting correction map.. ")
    //first reorder the calls to this:
    val ret = this.conn.createStatement().executeQuery(
      """SELECT
        |  callee,
        |  (array_agg(callstart ORDER BY callstart) || array_agg(callend ORDER BY callend)) as startend_times
        |FROM
        |  calls
        |WHERE
        |  name ='<init>'
        |GROUP BY callee
        |HAVING COUNT(*) >= 2;""".stripMargin)

    while (ret.next()) {
      // In the example above, times will be: [10,      30,      50,      20,     40,     60]
      //                                       A enter, B enter, C enter, A exit, B exit, C exit
      val times = ret
        .getArray("startend_times").getArray.asInstanceOf[Array[java.lang.Long]]
      this.conn.createStatement().execute(s"UPDATE calls SET callstart = ${times(0)} WHERE callend = ${times.last}")
      var i = 1
      while (i < times.length/2) {
        this.conn.createStatement().execute(s"DELETE FROM calls WHERE callend = ${times(times.length - 1 - i)}")
        i += 1
      }
    }
    ret.close()
    println(s" done (${watch.stop()})")
  }

  def computeEdgeEnds(): Unit = {
    val res = this.conn.createStatement().executeQuery(
      """
        |SELECT
        |  caller,
        |  name,
        |  array_agg(refstart ORDER BY refstart) AS refstarts,
        |  array_agg(kind     ORDER BY refstart) AS refkinds
        |FROM
        |  refs
        |WHERE kind = 'field'
        |GROUP BY
        |  caller, name
      """.stripMargin)

    var cnt = 0
    while(res.next()) {
      val caller = res.getLong("caller")
      val assignmentStarts = res.getArray("refstarts").getArray.asInstanceOf[Array[java.lang.Long]]
      val assignmentKinds  = res.getArray("refkinds").getArray.asInstanceOf[Array[String]]
      assert(assignmentStarts.length == assignmentKinds.length)
      val N = assignmentKinds.length
      var i = 0
      while (i<N-1) {
        closeEdge(caller, assignmentKinds(i), assignmentStarts(i), assignmentStarts(i+1))
        i = i+1
        cnt = cnt+1
      }
    }
    res.close()

    println(s"closed $cnt assignments")
  }

  def computeLastObjUsages(): Unit = {

    val firstLastUses = this.conn.createStatement().executeQuery(
      """SELECT
        |  callee,
        |  min(idx) AS firstusage,
        |  max(idx) AS lastusage
        |FROM uses
        |GROUP BY callee
        |""".stripMargin)
    while (firstLastUses.next()) {
      val id  = firstLastUses.getLong(1) // callee
      val fst = firstLastUses.getLong(2) // firstusage
      val lst = firstLastUses.getLong(3) // lastusage
      this.conn.createStatement().execute(
        s"""UPDATE objects
            |SET
            |  firstUsage = $fst,
            |  lastUsage  = $lst
            |WHERE id = $id""".stripMargin)
    }
    firstLastUses.close()
  }

  def storeClassFile(logDir: Path, file: File) {
    val relPath = logDir.relativize(file.toPath)

    val segments = for (i <- 0 until relPath.getNameCount) yield relPath.getName(i)
    val className = segments.mkString(".").replace(".class", "")
    val fileStream = new FileInputStream(file)

    val stat = this.conn.prepareStatement("INSERT INTO classdumps VALUES (?, ?)")
    stat.setString(1, className)
    stat.setBinaryStream(2, fileStream)
    stat.execute()
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
    var watch = Stopwatch.createStarted()
    val printBatchSize = 10e6
    var i : Long = 1
    while (events.hasNext) {
      val evt: AnyEvt.Reader = events.next
        if (!Util.isTrickyThreadName(EventsUtil.getThread(evt))) {
          handleEvent(evt, i)
        }
        if ((i-1) % printBatchSize == 0) {
          val evtsPerSec = if (i > 1) {
            Math.round(printBatchSize*1000.0/watch.elapsed(TimeUnit.MILLISECONDS))
          } else {
            "-"
          }
          watch.reset().start()
          println(
            s"#${((i-1) / 1e6).toFloat}e6 " +
            s"($evtsPerSec evts/sec)..")
        }
      i += 1
    }
    println("loading "+(i-1)+" events took "+stopwatch.stop())

    if (this.insertEdgeBatchSize > 0) {
      this.insertEdgeStatement.executeBatch()
    }
    if (this.finishEdgeBatchSize > 0) {
      this.finishEdgeStatement.executeBatch()
    }
    if (this.insertUseBatchSize > 0) {
      this.insertUseStatement.executeBatch()
    }
    this.conn.commit()

    createIndices()
    watch = Stopwatch.createStarted()
    print("sorting constructor calls... ")
    sortConstructorCalls()
    println(s"\tdone after ${watch.stop()}")

    watch.reset().start()

    print("computing edge ends... ")
    computeEdgeEnds()
    println(s"done after ${watch.stop()}")

    watch.reset().start()

    print("computing last obj usages... ")
    computeLastObjUsages()
    println(s"done after ${watch.stop()}")
    watch.reset().start()

    this.conn.commit()

    this.conn.createStatement().execute(
      "CREATE TABLE meta (key text UNIQUE, val text, PRIMARY KEY(key));")
    this.conn.createStatement().execute(
      s"""INSERT INTO meta
          |  (key, val)
          |VALUES
          |  ('comment', 'loaded from $path');
      """.stripMargin)
    val now = Calendar.getInstance()
    now.setLenient(false)
    this.conn.createStatement().execute(
      s"""INSERT INTO meta
        |  (key, val)
        |VALUES
        |  ('date', '${now.get(Calendar.YEAR)}-${"%02d".format(now.get(Calendar.MONTH) + 1)}-${now.get(Calendar.DAY_OF_MONTH)}');
      """.stripMargin)
    this.conn.createStatement().execute(
      s"""INSERT INTO meta
          |  (key, val)
          |VALUES
          |  ('eventcount', '${i-1}');
      """.stripMargin)
    this.conn.commit()

    val e = this.conn.createStatement().execute(
      """CREATE EXTENSION cstore_fdw;
        |CREATE SERVER cstore_server FOREIGN DATA WRAPPER cstore_fdw;
        |DROP FOREIGN TABLE IF EXISTS uses_cstore;
        |
        |CREATE FOREIGN TABLE uses_cstore (
        |  caller bigint,
        |  callee bigint,
        |  name text,
        |  method text,
        |  kind text,
        |  idx bigint,
        |  thread varchar(80),
        |  comment text)
        |SERVER cstore_server
        |OPTIONS(compression 'pglz');
        |
        |-- load negative callees first to improve skip index peformance:
        |INSERT INTO uses_cstore SELECT * FROM uses WHERE callee < 0;
        |INSERT INTO uses_cstore SELECT * FROM uses WHERE callee >= 0;
      """.stripMargin)
    this.conn.commit()

    cacheQueries()
    this.conn.commit()

    this.shutdown()
  }

  def cacheQueries(): Unit = {
    val qs = QueryParser.seriesOfQueries()
    for (q <- qs) {
      val time = Stopwatch.createStarted()
      print(s"pre caching $q.. ")
      this.prepareCaches(q.precacheInnersSQL)
      val res = this.getCachedOrRunSQL(q.cacheKey, q.getSQLUsingCache)
      this.getClassPercentage(q.toString)
      this.getFieldPercentage(q.toString)
      println(s"done after $time")
    }
    val del = QueryParser.wrapQueries("Not", QueryParser.seriesOfQueries())
    for (q <- del) {
      print(s"dropping $q.. ")
      this.conn.createStatement().execute(s"DROP TABLE IF EXISTS ${q.cacheKey};")
      this.conn.commit()
      println("done")
    }
  }

  def connect(): Unit = {
    this.connect_(false)
  }

  def connect_(overwrite: Boolean = false): Unit = {
    if (overwrite) {
      createFreshTables(this.dbname)
    }
    initPreparedStatements(this.dbname)
  }

  def initDbConnection(): Unit = {

    val opts = Map(
      "url"     -> s"jdbc:postgresql://postgres/$dbname",
      "dbtable" -> dbname,
      "user"    -> "spencer"
    )
    if (this.conn != null) {
      this.conn.close()
    }
    this.conn = DriverManager.getConnection(s"jdbc:postgresql://postgres/$dbname", System.getenv("POSTGRES_USER"), System.getenv("POSTGRES_PASSWORD"))
    this.conn.setAutoCommit(false)
  }

  def createFreshTables(dbname: String) {
    if (this.conn != null) {
      this.conn.close()
    }
    this.conn = DriverManager.getConnection("jdbc:postgresql://postgres/template1", System.getenv("POSTGRES_USER"), System.getenv("POSTGRES_PASSWORD"))
    this.conn.createStatement().execute(s"DROP DATABASE IF EXISTS $dbname")

    this.conn.createStatement().execute(s"CREATE DATABASE $dbname")
    initDbConnection()
    this.conn.createStatement().execute(
      """CREATE TABLE objects (
        |  id bigint UNIQUE,
        |  klass text,
        |  allocationsitefile text,
        |  allocationsiteline integer,
        |  firstUsage bigint UNIQUE,
        |  lastUsage bigint UNIQUE,
        |  thread text,
        |  PRIMARY KEY(id))""".stripMargin)

    this.conn.createStatement().execute(
      """CREATE TABLE refs (
        |  caller bigint,
        |  callee bigint,
        |  kind varchar(10),
        |  name varchar(80),
        |  refstart bigint UNIQUE,
        |  refend bigint,
        |  thread text,
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
        |  thread) VALUES (?, ?, ?, ?, ?)
        |  ON CONFLICT(id) DO UPDATE SET klass = EXCLUDED.klass;""".stripMargin)
    this.insertEdgeStatement = this.conn.prepareStatement(
      """INSERT INTO refs (
        |  caller,
        |  callee,
        |  kind,
        |  name,
        |  thread,
        |  refstart) VALUES (?, ?, ?, ?, ?, ? )""".stripMargin)
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
        |  thread) VALUES (?, ?, ?, ?, ?, ?, ? )""".stripMargin)
    this.insertCallStatement = this.conn.prepareStatement(
      """INSERT INTO calls (
        |  caller,
        |  callee,
        |  name,
        |  callstart,
        |  callend,
        |  thread,
        |  callsitefile,
        |  callsiteline) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin)
  }
}

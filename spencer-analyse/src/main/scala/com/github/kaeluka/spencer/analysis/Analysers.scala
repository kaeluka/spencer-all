package com.github.kaeluka.spencer.analysis

import java.io._
import java.nio.charset.StandardCharsets
import java.sql.{ResultSet, ResultSetMetaData}

import com.github.kaeluka.spencer.PostgresSpencerDB
import com.google.common.base.Stopwatch
import org.objectweb.asm.ClassReader
import org.objectweb.asm.util.TraceClassVisitor

object SQLUtil {
  def indentedInner(inner : String, lvl : Int): String = {
    val lines = inner.lines
    assert(lines.hasNext)
    var ret = lines.next()
    while (lines.hasNext) {
      ret = ret + "\n" + (" " * lvl) + lines.next()
    }
    ret
  }

  def shorten(maxLen : Int, str : String) : String = {
    var ret = str
    if (ret.length > maxLen) {
      val snippetLen = maxLen/2 - 1
      ret = ret.substring(0, snippetLen)+".."+ret.substring(ret.length - snippetLen, ret.length)
    }
    ret
  }

  def fixedLength(len : Int, str : String) : String = {
    if (str.length < len) {
      str+(" "*(len - str.length))
    } else {
      shorten(len, str)
    }
  }

  def print(r: ResultSet, maxRows: Int = 10) = {
    val rsmd = r.getMetaData
    val columnsNumber = rsmd.getColumnCount
    val columns = collection.mutable.ArrayBuffer[String]()

    {
      var i = 1
      while (i <= columnsNumber) {
        columns += rsmd.getColumnName(i)
        i += 1
      }
    }
    var row = "|"
    for (col <- columns) {
      var cell = fixedLength(30, col).toUpperCase()
      row = row + cell + "\t|\t"
    }
    println(row)
    var rows = 0
    while (r.next && rows < maxRows) {
      rows += 1
      var row = "|"
      for (col <- columns) {
        var cell = fixedLength(30, r.getString(col))
        row = row + cell + "\t|\t"
      }
      println(row)
    }

    if (r.next) {
      rows += 1
      println("...")
      while (r.next) {
        rows += 1
      }
    }
    println(s"TOTAL: $rows rows selected")
  }
}

trait SpencerAnalyser[T] {
  def analyse(implicit g: PostgresSpencerDB) : T
}

trait ResultSetAnalyser extends SpencerAnalyser[ResultSet] {
  override def analyse(implicit g: PostgresSpencerDB) : ResultSet = {
    g.getCachedOrRunQuery(this)
  }

  def explanation(): String

  def getInners: Seq[SpencerQuery] = List()

  def getVersion: Int

  /**
    * Produce a dependency tree (JSON) in this format:
    * {
    *  "And(StationaryObj() And(MutableObj() UniqueObj()))": {
    *    "StationaryObj()": {
    *      "NonStationaryObj()": { cache: "cache__1837766281_0" }
    *      cache: "cache_563859730_0"
    *    },
    *    "And(MutableObj() UniqueObj())": {
    *      "MutableObj()": { cache: "cache_39765202_0" },
    *      "UniqueObj()": { cache: "cache__267503929_0" }
    *      cache: "cache__1300376285_0"
    *    }
    *    cache: "cache__982222453_0"
    *  }
    *}
    * @return the dependency tree of the query, formatted as JSON
    */
  def dependencyTree() : String = {
    s"""{
        |  ${SQLUtil.indentedInner(dependencyTree_(), 2)}
        |}""".stripMargin
  }

  def dependencyTree_() : String = {
    val ret = if (this.getInners.size > 0) {
      val deps = SQLUtil.indentedInner(this.getInners.map(inner => inner.dependencyTree_()).mkString(",\n"), 2)
      s"""\"${this.toString}\": {
          |  $deps
          |}""".stripMargin
    } else {
      s"""\"${this.toString}\": { }"""
    }
    ret
  }

  /**
    * Gives a sequence of SQL commands that can pre-cache the results.
    * If the cache already exists, these queries will *not* rerun it
    * @return a sequence of commands (as Strings) that can pre-cache the results
    *         of this query. use getSQLUsingCache to load the cached query
    */
  final def precacheInnersSQL = {
    val x = getInners.map({
      inner =>
        s"""CREATE TABLE IF NOT EXISTS ${inner.cacheKey} AS (
            |  ${AnaUtil.indent(inner.getSQL, 2)}
            |);""".stripMargin
    })
    x
  }

  def replaceQuestionMark(blueprint: String, inner : String) : String = {
    val lines = blueprint.lines
    var done = false
    var ret = null : String
    while (lines.hasNext && ! done) {
      val line = lines.next()
      val i = line.indexOf('?')
      if (i >= 0) {
        ret = blueprint.replaceFirst("\\?", SQLUtil.indentedInner(inner, i))
        done = true
      }
    }

    assert(ret != null)
    ret
  }

  def getSQL: String = {
    var blueprint = getSQLBlueprint
    val inners = getInners
    assert(blueprint.count(_ == '?') == getInners.size)
    for (inner <- inners) {
      blueprint = replaceQuestionMark(blueprint, inner.getSQL)
    }
    blueprint
  }

  def getSQLUsingCache: String = {
    assert(this.getSQLBlueprint.count(_ == '?') == getInners.size)
    var assembledSQL = getSQLBlueprint
    for (inner <- this.getInners) {
      assembledSQL = replaceQuestionMark(assembledSQL, inner.getCacheSQL)
    }
    assembledSQL
  }

  def getCacheSQL: String = s"SELECT id FROM ${this.cacheKey}"

  def getSQLBlueprint: String

  def cacheKey: String = {
    val innerKeys = this.getInners.map(_.cacheKey).mkString("")
    val thisKey = (innerKeys+this.toString).hashCode
    s"cache_${thisKey.hashCode.toString.replaceAll("-","_")}_${this.getVersion}"
  }
}

case class Apropos(id: Long) extends SpencerAnalyser[AproposData] {
  override def analyse(implicit g: PostgresSpencerDB): AproposData = {
    g.aproposObject(id)
  }
}

case class SourceCode(klass: String) extends SpencerAnalyser[Option[String]] {
  override def analyse(implicit g: PostgresSpencerDB): Option[String] = {
    val resultSet = g.runSQLQuery(s"SELECT bytecode FROM classdumps WHERE classname = '$klass'")
    val ret = if (resultSet.next()) {
      val bytecode = resultSet.getBytes("bytecode")
      assert(!resultSet.next, s"Have several ambiguous bytecodes for class $klass")
      val classreader = new ClassReader(bytecode)
      val baos = new ByteArrayOutputStream()
      val sw : PrintWriter = new PrintWriter(new PrintStream(baos))
      classreader.accept(new TraceClassVisitor(sw), ClassReader.EXPAND_FRAMES)
      Some(new String(baos.toByteArray, StandardCharsets.UTF_8))
    } else {
      None
    }
    resultSet.close()
    ret
  }
}

// an aggregator is an analyser that selects at least the columns
//  - group (text),
//  - passed (int),
//  - total (int),
//  - perc (real)
trait Aggregator extends ResultSetAnalyser { }

case class FieldAggregator(inner: SpencerQuery) extends Aggregator {

  override def getSQLBlueprint: String = {
    s"""-- select all fields, the number of objects, the number of passed objects, and the percentage
       |SELECT
       |  total.field                                                AS name,
       |  total.ncallees                                             AS total,
       |  COALESCE(passes.ncallees,0)                                AS passed,
       |  round(100.0*COALESCE(passes.ncallees,0)/total.ncallees, 2) AS perc
       |FROM (
       |  -- select all fields, and the number of objects that were referenced from them
       |  SELECT
       |    CONCAT(callerObjs.klass,'::',name) field, COUNT(DISTINCT callee) ncallees
       |  FROM refs INNER JOIN objects calleeObjs ON calleeObjs.id = refs.callee INNER JOIN objects callerObjs ON callerObjs.id = refs.caller
       |  WHERE kind = 'field'
       |  AND   callerObjs.klass NOT LIKE '[%'
       |  AND   refs.callee IN (?)
       |  GROUP BY (callerObjs.klass, refs.name)
       |) AS total
       |LEFT OUTER JOIN (
       |  -- select all fields, and the number of objects that passed that were referenced
       |  -- from them
       |  SELECT
       |    CONCAT(callerObjs.klass,'::',name) field, COUNT(DISTINCT callee) ncallees
       |  FROM refs INNER JOIN objects calleeObjs ON calleeObjs.id = refs.callee INNER JOIN objects callerObjs ON callerObjs.id = refs.caller
       |  WHERE kind = 'field'
       |  AND   callerObjs.klass NOT LIKE '[%'
       |  AND   refs.callee IN (?)
       |  GROUP BY (callerObjs.klass, name)
       |) AS passes
       |ON total.field = passes.field
       |ORDER BY perc DESC""".stripMargin
  }

  override def explanation() = s"Field aggregation of objects that ${inner.explanation()}"

  override def getVersion = 0

  override def getInners = List(Obj(), inner)

}

object AnalysersTest extends App {

  implicit val db: PostgresSpencerDB = new PostgresSpencerDB("test")
  db.connect()

  val watch: Stopwatch = Stopwatch.createStarted()
  val q = FieldAggregator(QueryParser.parseObjQuery("MutableObj()").right.get)
  println(q.toString)
  println(s"getSQL:\n${q.getSQL}")
  println(s"precacheInnersSQL:\n${q.precacheInnersSQL.mkString("\n")}")
  println(s"getSQLUsingCache:\n${q.getSQLUsingCache}")
  println(s"getCacheSQL: ${q.getCacheSQL}")
  println(s"dependencyTree:\n${q.dependencyTree()}")
  val res = db.runSQLQuery(q.getSQL)
//  val res = q.analyse

  println(
    s"""analysis took ${watch.stop()}
        |getSQL:\n${q.getSQL}
        |precacheInnersSQL:\n${q.precacheInnersSQL.mkString("\n")}
        |getSQL:\n${q.getSQL}""".stripMargin)

  SQLUtil.print(res, 1000)
}

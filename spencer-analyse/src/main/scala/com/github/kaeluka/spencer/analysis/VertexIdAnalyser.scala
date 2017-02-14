package com.github.kaeluka.spencer.analysis

import com.github.kaeluka.spencer.PostgresSpencerDB
import com.github.kaeluka.spencer.analysis.EdgeKind.EdgeKind
import com.google.common.base.Stopwatch
import org.apache.spark.graphx._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

trait VertexIdAnalyser extends SpencerAnalyser[DataFrame] {

  def snapshotted() : VertexIdAnalyser = {
    SnapshottedVertexIdAnalyser(this)
  }

  override def analyse(implicit g: PostgresSpencerDB): DataFrame = {
    g.prepareCaches(this.precacheInnersSQL)
    g.getCachedOrRunQuery(this)
    g.selectFrame(this.cacheKey, this.getCacheSQL)
  }

  override def pretty(result: DataFrame): String = {
    val N = result.count()
    val resString = if (N > 100) {
      result.take(100).toString
    } else {
      result.toString()
    }
    this.explanation()+":\n"+resString
  }

  /**
    * Gives a sequence of SQL commands that can pre-cache the results.
    * @return
    */
  def getInners: Seq[VertexIdAnalyser] = List()

  def precacheInnersSQL = {
    val x = getInners.map({
      inner =>
        s"""CREATE TABLE IF NOT EXISTS ${inner.cacheKey} AS (
           |  ${AnaUtil.indent(inner.getSQL, 2)}
           |);""".stripMargin
    })
    x
  }

  def getSQL: String = {
    var blueprint = getSQLBlueprint
    val inners = getInners
    assert(blueprint.count(_ == '?') == getInners.size)
    for (inner <- inners) {
      blueprint = blueprint.replaceFirst("\\?", inner.getSQL)
    }
    blueprint
  }

  def getSQLUsingCache: String = {
    assert(this.getSQLBlueprint.count(_ == '?') == getInners.size)
    var assembledSQL = getSQLBlueprint
    for (inner <- this.getInners) {
      assembledSQL = assembledSQL.replaceFirst("\\?", inner.getCacheSQL)
    }
    assembledSQL
  }

  def getCacheSQL: String = s"SELECT id FROM ${this.cacheKey}"

  def getSQLBlueprint: String

  def cacheKey: String = s"cache_${this.toString.hashCode.toString.replaceAll("-","_")}"
}

object AnaUtil {
  def indent(s: String, lvl: Int): String = {
    s.split("\n").mkString("\n"+(" "*lvl))
  }
}

object And {
  def apply(vs: Seq[VertexIdAnalyser]) : VertexIdAnalyser = {
    val vs_ = vs
      .filter(_.toString != "Obj()")
      .flatMap({
        case _And(innerVs) => innerVs
        case other => List(other)
      })
    assert(vs_.nonEmpty)
    if (vs_.size == 1) {
      vs_.head
    } else {
      _And(vs_)
    }
  }
}

case class _And(vs: Seq[VertexIdAnalyser]) extends VertexIdAnalyser {

  override def explanation(): String = vs.map(_.explanation()).mkString(", and ")

  override def toString: String = vs.mkString("And(", " ", ")")

  override def getInners = vs

  override def precacheInnersSQL: Seq[String] = {
    vs.flatMap(_.precacheInnersSQL)
  }

  override def getSQLBlueprint: String = {
    vs.map(_ => "  ?").mkString("(\n", "\n) INTERSECT (\n", "\n)")
  }
}

object Or {
  def apply(vs_ : Seq[VertexIdAnalyser]) : VertexIdAnalyser = {
    vs_.find(_.toString == "Obj()") match {
      case Some(q) => q
      case None =>
        val vs = vs_
          .flatMap({
            case _Or(innerVs) => innerVs
            case other => List(other)
          })
        _Or(vs)
    }
  }
}

case class _Or(vs: Seq[VertexIdAnalyser]) extends VertexIdAnalyser {

  override def explanation(): String = vs.map(_.explanation()).mkString(", or ")

  override def toString: String = vs.mkString("Or(", " ", ")")

  override def getInners = vs

  override def getSQLBlueprint = {
    vs.map(_ => "  ?").mkString("(\n", "\n) UNION (\n", "\n)")
  }
}

case class SnapshottedVertexIdAnalyser(inner : VertexIdAnalyser) extends VertexIdAnalyser {

  override def snapshotted(): VertexIdAnalyser = this

  override def pretty(result: DataFrame): String = inner.pretty(result)

  override def toString: String = inner.toString

  override def getInners = inner.getInners

  override def explanation(): String = inner.explanation()

  override def getSQLBlueprint = inner.getSQLBlueprint
}

/**
  * filters for all objects that only ever have references to objects older than
  * them
  */
case class ReverseAgeOrderedObj() extends VertexIdAnalyser {

  override def explanation(): String = {
    "are only holding field references to objects created after them"
  }

  override def getSQLBlueprint = {
    s"""SELECT id FROM
       |  (${AnaUtil.indent(AgeOfNeighbours().getSQLBlueprint,3)}) AS AgeOfNeigbhours
       |GROUP BY id, firstusage
       |HAVING MAX(calleefirstusage) > firstusage""".stripMargin
  }
}

/**
  * filters for all objects that only ever have references to objects younger than
  * them
  */
case class AgeOrderedObj() extends VertexIdAnalyser {

  override def explanation(): String = {
    "are only holding field references to objects created before them"
  }

  override def getSQLBlueprint = {
    s"""SELECT id FROM
       |  (${AnaUtil.indent(AgeOfNeighbours().getSQLBlueprint,3)}) AS AgeOfNeigbhours
       |GROUP BY id, firstusage
       |HAVING MAX(calleefirstusage) < firstusage""".stripMargin
  }
}

case class AgeOfNeighbours() extends VertexIdAnalyser {

  override def explanation(): String = "Age"

  override def getSQLBlueprint = {
    """SELECT
     |  objects.id         AS id,
     |  objects.firstusage AS firstusage,
     |  callees.firstusage AS calleefirstusage
     |FROM objects
     |INNER JOIN refs               ON objects.id = refs.caller
     |INNER JOIN objects AS callees ON refs.callee = callees.id
     |WHERE
     |  refs.kind = 'field'""".stripMargin
  }
}

case class MutableObj() extends VertexIdAnalyser {

  override def explanation(): String = "are changed outside their constructor"

  override def getSQLBlueprint = {
    """SELECT DISTINCT callee AS id
     |FROM uses_cstore
     |WHERE
     |  callee > 4 AND
     |  NOT(caller = callee AND method = '<init>') AND
     |  (kind = 'fieldstore' OR kind = 'modify')""".stripMargin
  }
}

object ThreadLocalObj {
  def apply() : VertexIdAnalyser = {
    Named(IsNot(NonThreadLocalObj()), "ThreadLocalObj()", "are accessed by only one thread")
  }
}

case class NonThreadLocalObj() extends VertexIdAnalyser {

  override def explanation(): String = "are changed outside their constructor"

  override def getSQLBlueprint = {
    """SELECT callee
      |FROM uses_cstore
      |WHERE callee > 4
      |GROUP BY callee
      |HAVING COUNT(DISTINCT thread) > 1
      |""".stripMargin
  }
}

case class UniqueObj() extends VertexIdAnalyser {
  override def getSQLBlueprint = {
    """SELECT callee AS id FROM
      |(SELECT callee, time, SUM(delta) OVER(PARTITION BY callee ORDER BY time) AS sum_at_time
      | FROM (
      |   (SELECT
      |      callee, refstart AS time, 1 AS delta
      |    FROM refs
      |    WHERE callee > 4) UNION ALL (SELECT
      |      callee, refend AS time, -1 AS delta
      |    FROM refs
      |    WHERE callee > 4)
      | ) AS steps) AS integrated_steps
      |GROUP BY callee
      |HAVING MAX(sum_at_time) = 1""".stripMargin
  }

  override def explanation(): String = "are never aliased"
}

case class HeapUniqueObj() extends VertexIdAnalyser {
  override def getSQLBlueprint = {
    """SELECT callee AS id FROM
      |(SELECT callee, time, SUM(delta) OVER(PARTITION BY callee ORDER BY time) AS sum_at_time
      | FROM (
      |   (SELECT
      |      callee, refstart AS time, 1 AS delta
      |    FROM refs
      |    WHERE callee > 4 AND kind = 'field') UNION ALL (SELECT
      |      callee, refend AS time, -1 AS delta
      |    FROM refs
      |    WHERE callee > 4 AND kind = 'field')
      | ) AS steps) AS integrated_steps
      |GROUP BY callee
      |HAVING MAX(sum_at_time) = 1""".stripMargin
  }

  override def explanation(): String = "are never aliased"
}

case class StackBoundObj() extends VertexIdAnalyser {
  override def getSQLBlueprint = {
    """SELECT id FROM objects EXCEPT (SELECT callee FROM
      |  (SELECT
      |     callee,
      |     time,
      |     SUM(delta) OVER(PARTITION BY callee ORDER BY time) AS sum_at_time
      |   FROM (
      |     (SELECT callee, refstart AS time, 1 AS delta
      |      FROM refs
      |      WHERE callee > 4 AND kind = 'field') UNION ALL (SELECT
      |        callee, refend AS time, -1 AS delta
      |      FROM refs
      |      WHERE callee > 4 AND kind = 'field')
      |   ) AS steps) AS integrated_steps
      |GROUP BY callee
      |HAVING MAX(sum_at_time) > 0)""".stripMargin
  }

  override def explanation(): String = "are never aliased"
}

case class ImmutableObj() extends VertexIdAnalyser {

  override def explanation(): String = "are never changed outside their constructor"

  override def getInners = IsNot(MutableObj()).getInners

  override def getSQLBlueprint: String = {
    IsNot(MutableObj()).getSQLBlueprint
  }
}

case class StationaryObj() extends VertexIdAnalyser {

  override def explanation(): String = "are never changed after being read from for the first time"

  override def getSQLBlueprint = {
    """SELECT idx, caller, callee, kind
      |FROM uses_cstore read
      |WHERE callee > 4
      |AND   (kind = 'fieldload' OR kind = 'read')
      |AND   EXISTS (SELECT 1
      |              -- cstore would be worse for random lookup!
      |              FROM   uses store
      |              WHERE  store.callee = read.callee
      |              AND    (kind = 'fieldstore' OR kind = 'modify')
      |              AND    store.idx > read.idx)
      |""".stripMargin
  }
}

case class Obj() extends VertexIdAnalyser {

  override def explanation(): String = "were traced"

  override def getSQLBlueprint = {
    "SELECT id FROM objects WHERE id >= 4"
  }
}

case class AllocatedAt(allocationSite: (String, Long)) extends VertexIdAnalyser {

  override def toString: String = {
    "AllocatedAt("+allocationSite._1+":"+allocationSite._2.toString+")"
  }

  override def explanation(): String = "were allocated at "+allocationSite._1+":"+allocationSite._2

  override def getSQLBlueprint = {
    s"""SELECT id FROM objects WHERE
       |allocationsitefile = '${allocationSite._1}' AND
       |allocationsiteline = ${allocationSite._2}""".stripMargin
  }
}

case class InstanceOf(klassName: String) extends VertexIdAnalyser {

  def this(klass: Class[_]) =
    this(klass.getName)

  override def explanation(): String = "are instances of class "+klassName

  override def getSQLBlueprint = {
    s"""SELECT id FROM objects WHERE klass = '$klassName'""".stripMargin
  }
}

case class IsNot(inner: VertexIdAnalyser) extends VertexIdAnalyser {

  override def explanation(): String = "not "+inner.explanation()

  override def getInners = List(inner)

  override def getSQLBlueprint = {
    s"""SELECT id FROM objects WHERE id > 4
        |EXCEPT
        |  (?)
      """.stripMargin
  }
}

object Named {
  def apply(inner: VertexIdAnalyser, name: String) = {
    new Named(inner, name)
  }
}
case class Named(inner: VertexIdAnalyser, name: String, expl: String) extends VertexIdAnalyser {

  def this(inner: VertexIdAnalyser, name: String) =
    this(inner, name, inner.explanation())

  override def pretty(result: DataFrame): String = inner.pretty(result)

  override def toString: String = name

  override def getInners = List(inner)

  override def explanation(): String = this.expl

  override def getSQLBlueprint = {
    inner.getSQLBlueprint
  }
}

case class Deeply(inner: VertexIdAnalyser,
                  edgeFilter : Option[EdgeKind] = None) extends VertexIdAnalyser {
  override def explanation(): String = {
    inner.explanation()+", and the same is true for all reachable objects"
  }

  override def getInners = List(inner)

  override def getSQLBlueprint = {
    val reachability = edgeFilter match {
      case None => CanReach
      case Some(EdgeKind.FIELD) => CanHeapReach
    }
    IsNot(reachability(IsNot(inner))).getSQLBlueprint
  }
}

case class ConstSeq(value: Seq[VertexId]) extends VertexIdAnalyser {
  override def analyse(implicit g: PostgresSpencerDB): DataFrame = {
    import g.sqlContext.implicits._
    g.sqlContext.sparkSession.sparkContext.parallelize(value).toDF().withColumnRenamed("value", "id")
  }

  override def pretty(result: DataFrame): String = {
    value.mkString("[ ", ", ", " ]")
  }

  override def explanation(): String = "any of "+value.mkString("{", ", ", "}")

  override def getSQLBlueprint = {
    value.mkString("SELECT * FROM (VALUES (", "", s") AS const${this.cacheKey}(id)")
  }
}

case class Const(value: DataFrame) extends VertexIdAnalyser {
  override def analyse(implicit g: PostgresSpencerDB):DataFrame = value

  override def pretty(result: DataFrame): String = this.toString

  override def explanation(): String = "constant set "+value.toString

  override def getSQLBlueprint = {
    ??? //FIXME
  }
}

case class HeapRefersTo(inner: VertexIdAnalyser) extends VertexIdAnalyser {

  override def explanation(): String = "are field-referring to objects that "+inner.explanation()

  override def getInners = List(inner)

  override def getSQLBlueprint = {
    s"""SELECT
       |  callee AS id
       |FROM
       |  refs
       |WHERE
       |  kind = 'field' AND
       |  callee IN (
       |    ?
       |  )""".stripMargin
  }
}

case class RefersTo(inner: VertexIdAnalyser) extends VertexIdAnalyser {

  override def explanation(): String = "are referring to objects that "+inner.explanation()

  override def getInners = List(inner)

  override def getSQLBlueprint = {
    s"""SELECT
       |  callee AS id
       |FROM
       |  refs
       |WHERE
       |  callee IN (
       |    ?
       |  )""".stripMargin
  }
}

case class HeapReferredFrom(inner: VertexIdAnalyser) extends VertexIdAnalyser {

  override def explanation(): String = "are heap-referred to from objects that "+inner.explanation()

  override def getInners = List(inner)

  override def getSQLBlueprint = {
    s"""SELECT
       |  callee AS id
       |FROM
       |  refs
       |WHERE
       |  kind = 'field' AND
       |  caller IN (
       |    ?
       |  )""".stripMargin
  }
}

case class ReferredFrom(inner: VertexIdAnalyser) extends VertexIdAnalyser {

  override def explanation(): String = "are referred to from objects that "+inner.explanation()

  override def getInners = List(inner)

  override def getSQLBlueprint = {
    s"""SELECT
       |  callee AS id
       |FROM
       |  refs
       |WHERE
       |  caller IN (
       |    ?
       |  )""".stripMargin
  }
}

case class HeapReachableFrom(inner: VertexIdAnalyser) extends VertexIdAnalyser {
  override def getInners = List(inner)

  override def getSQLBlueprint = {
    s"""WITH RECURSIVE heapreachablefrom(id) AS (
       |    ?
       |  UNION
       |    SELECT
       |      refs.callee AS id
       |    FROM refs
       |    JOIN heapreachablefrom ON heapreachablefrom.id = refs.caller
       |    WHERE kind = 'field'
       |)
       |SELECT id FROM heapreachablefrom""".stripMargin
  }

  override def explanation(): String = s"are heap-reachable from objects that ${inner.explanation()}"
}

case class ReachableFrom(inner: VertexIdAnalyser) extends VertexIdAnalyser {
  override def getInners = List(inner)

  override def getSQLBlueprint = {
    s"""WITH RECURSIVE reachablefrom(id) AS (
       |    ?
       |  UNION
       |    SELECT
       |      refs.callee AS id
       |    FROM refs
       |    JOIN reachablefrom ON reachablefrom.id = refs.caller
       |)
       |SELECT id FROM reachablefrom""".stripMargin
  }

  override def explanation(): String = s"are reachable from objects that ${inner.explanation()}"
}

case class CanHeapReach(inner: VertexIdAnalyser) extends VertexIdAnalyser {
  override def getInners = List(inner)

  override def getSQLBlueprint = {
    s"""WITH RECURSIVE canheapreach(id) AS (
       |    ?
       |  UNION
       |    SELECT
       |      refs.caller AS id
       |    FROM refs
       |    JOIN canheapreach ON canheapreach.id = refs.callee
       |    WHERE kind = 'field'
       |)
       |SELECT id FROM canheapreach""".stripMargin
  }

  override def explanation(): String = s"are able to heap-reach objects that ${inner.explanation()}"
}

case class CanReach(inner: VertexIdAnalyser) extends VertexIdAnalyser {
  override def getInners = List(inner)

  override def getSQLBlueprint = {
    s"""WITH RECURSIVE canreach(id) AS (
       |    ?
       |  UNION
       |    SELECT
       |      refs.caller AS id
       |    FROM refs
       |    JOIN canreach ON canreach.id = refs.callee
       |)
       |SELECT id FROM canreach""".stripMargin
  }

  override def explanation(): String = s"are able to reach objects that ${inner.explanation()}"
}

case class TinyObj() extends VertexIdAnalyser {

  override def explanation(): String = "do not have or do not use reference type fields"

  override def getSQLBlueprint = {
    s"""(${AnaUtil.indent(Obj().getSQLBlueprint,1)})
       |  EXCEPT
       |  (SELECT
       |     DISTINCT caller
       |   FROM
       |     refs
       |   WHERE
       |     kind = 'field')""".stripMargin
  }
}

object VertexIdAnalyserTest extends App {

  implicit val db: PostgresSpencerDB = new PostgresSpencerDB("test")
  db.connect()

  val watch: Stopwatch = Stopwatch.createStarted()
  val q = StationaryObj()
  println(s"getSQL:\n${q.getSQL}")
  println(s"precacheInnersSQL:\n${q.precacheInnersSQL.mkString("\n")}")
  println(s"getSQLUsingCache:\n${q.getSQLUsingCache}")
  println(s"getCacheSQL: ${q.getCacheSQL}")
  val res = q.analyse //AgeOrderedObj().analyse

  res.repartition()
  res.show()
  println(
    s"""analysis took ${watch.stop()}
       |got ${res.count} objects
       |getSQL:\n${q.getSQL}
       |precacheInnersSQL:\n${q.precacheInnersSQL.mkString("\n")}
       |getSQLUsingCache:\n${q.getSQLUsingCache}""".stripMargin)
}

package com.github.kaeluka.spencer.analysis

import java.sql.ResultSet

import com.github.kaeluka.spencer.PostgresSpencerDB
import com.github.kaeluka.spencer.analysis.EdgeKind.EdgeKind
import com.google.common.base.Stopwatch

// A spencer query is an analyser that contains the column 'id' (bigint).
trait SpencerQuery extends ResultSetAnalyser { }

object AnaUtil {
  def indent(s: String, lvl: Int): String = {
    s.split("\n").mkString("\n"+(" "*lvl))
  }
}

case class And(vs: Seq[SpencerQuery]) extends SpencerQuery {

  override def explanation(): String = vs.map(_.explanation()).mkString(", and ")

  override def toString: String = vs.mkString("And(", " ", ")")

  override def getInners = vs

  override def getSQLBlueprint: String = {
    vs.map(_ => "  ?").mkString("(\n", "\n) INTERSECT (\n", "\n)")
  }

  override def getVersion: Int = { 0 }
}

case class Or(vs: Seq[SpencerQuery]) extends SpencerQuery {

  override def explanation(): String = vs.map(_.explanation()).mkString(", or ")

  override def toString: String = vs.mkString("Or(", " ", ")")

  override def getInners = vs

  override def getSQLBlueprint = {
    vs.map(_ => "  ?").mkString("(\n", "\n) UNION (\n", "\n)")
  }

  override def getVersion: Int = { 0 }
}

/**
  * filters for all objects that only ever have references to objects older than
  * them
  */
case class ReverseAgeOrderedObj() extends SpencerQuery {

  override def explanation(): String = {
    "are only holding field references to objects created after them"
  }

  override def getSQLBlueprint = {
    s"""SELECT id FROM
       |  (${AnaUtil.indent(AgeOfNeighbours().getSQLBlueprint,3)}) AS AgeOfNeighbours
       |GROUP BY id, firstusage
       |HAVING MAX(calleefirstusage) > firstusage""".stripMargin
  }

  override def getVersion: Int = { 0 }
}

/**
  * filters for all objects that only ever have references to objects younger than
  * them
  */
case class AgeOrderedObj() extends SpencerQuery {

  override def explanation(): String = {
    "are only holding field references to objects created before them"
  }

  override def getSQLBlueprint = {
    s"""SELECT id FROM
       |  (${AnaUtil.indent(AgeOfNeighbours().getSQLBlueprint,3)}) AS AgeOfNeighbours
       |GROUP BY id, firstusage
       |HAVING MAX(calleefirstusage) < firstusage""".stripMargin
  }

  override def getVersion: Int = { 0 }
}

case class AgeOfNeighbours() extends SpencerQuery {

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

  override def getVersion: Int = { 0 }
}

case class MutableObj() extends SpencerQuery {

  override def explanation(): String = "are changed outside their constructor"

  override def getSQLBlueprint = {
    """SELECT DISTINCT callee AS id
      |FROM   uses_cstore
      |WHERE  callee > 4
      |AND    NOT(caller = callee AND method = '<init>')
      |AND    (kind = 'fieldstore' OR kind = 'modify')""".stripMargin
  }

  override def getVersion: Int = { 0 }
}

object ThreadLocalObj {
  def apply() : SpencerQuery = {
    Named(Not(NonThreadLocalObj()), "ThreadLocalObj()", "are accessed by only one thread")
  }
}

case class NonThreadLocalObj() extends SpencerQuery {

  override def explanation(): String = "are changed outside their constructor"

  override def getSQLBlueprint = {
    """SELECT callee
      |FROM uses_cstore
      |WHERE callee > 4
      |GROUP BY callee
      |HAVING COUNT(DISTINCT thread) > 1""".stripMargin
  }

  override def getVersion: Int = { 0 }
}

case class UniqueObj() extends SpencerQuery {
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

  override def getVersion: Int = { 0 }
}

case class HeapUniqueObj() extends SpencerQuery {
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

  override def getVersion: Int = { 0 }
}

case class StackBoundObj() extends SpencerQuery {
  override def getSQLBlueprint = {
    """SELECT id
      |FROM   objects
      |WHERE  id > 4
      |AND    NOT EXISTS (
      |         SELECT 1
      |         FROM   refs
      |         WHERE  refs.callee = objects.id
      |         AND    refs.kind = 'field'
      |       )""".stripMargin
  }

  override def explanation(): String = "are never aliased"

  override def cacheKey: String = super.cacheKey+"_v2"

  override def getVersion = { 0 }
}

case class ImmutableObj() extends SpencerQuery {

  override def explanation(): String = "are never changed outside their constructor"

  override def getInners = Not(MutableObj()).getInners

  override def getSQLBlueprint: String = {
    Not(MutableObj()).getSQLBlueprint
  }

  override def getVersion = { 0 }
}

case class StationaryObj() extends SpencerQuery {
  val inner = Not(NonStationaryObj())
  override def getSQLBlueprint = {
    inner.getSQLBlueprint
  }

  override def getInners = inner.getInners

  override def explanation(): String = "are never changed after being read from"

  override def getVersion = { 0 }
}


case class NonStationaryObj() extends SpencerQuery {

  override def explanation(): String = "are changed after being read from"

  override def getSQLBlueprint = {
    """SELECT idx
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

  override def getVersion = { 0 }
}

case class Obj() extends SpencerQuery {

  override def explanation(): String = "were traced"

  override def getSQLBlueprint = {
    "SELECT id FROM objects WHERE id >= 4"
  }

  override def getVersion = { 0 }
}

case class AllocatedAt(allocationSite: (String, Long)) extends SpencerQuery {

  override def toString: String = {
    "AllocatedAt("+allocationSite._1+":"+allocationSite._2.toString+")"
  }

  override def explanation(): String = "were allocated at "+allocationSite._1+":"+allocationSite._2

  override def getSQLBlueprint = {
    s"""SELECT id FROM objects
       |WHERE  allocationsitefile = '${allocationSite._1}'
       |AND    allocationsiteline = ${allocationSite._2}""".stripMargin
  }

  override def getVersion = { 0 }
}

case class InstanceOf(klassName: String) extends SpencerQuery {

  def this(klass: Class[_]) =
    this(klass.getName)

  override def explanation(): String = "are instances of class "+klassName

  override def getSQLBlueprint = {
    s"""SELECT id FROM objects WHERE klass = '$klassName'""".stripMargin
  }

  override def getVersion = { 0 }
}

object Not {
  def apply(inner: SpencerQuery) : SpencerQuery = {
    inner match {
      case n: Not_ => n.inner
      case _       => Not_(inner)
    }
  }
}
case class Not_(inner: SpencerQuery) extends SpencerQuery {

  override def explanation(): String = "not "+inner.explanation()

  override def getInners = List(inner)

  override def getSQLBlueprint = {
    s"""SELECT id FROM objects WHERE id > 4
        |EXCEPT
        |  (?)
      """.stripMargin
  }

  override def toString = s"Not(${inner.toString})"

  override def getVersion = { 0 }
}

object Named {
  def apply(inner: SpencerQuery, name: String) = {
    new Named(inner, name)
  }
}
case class Named(inner: SpencerQuery, name: String, expl: String) extends SpencerQuery {

  def this(inner: SpencerQuery, name: String) =
    this(inner, name, inner.explanation())

  override def toString: String = name

  override def getInners = List(inner)

  override def explanation(): String = this.expl

  override def getSQLBlueprint = {
    inner.getSQLBlueprint
  }

  override def cacheKey: String = inner.cacheKey

  override def getVersion = { 0 }
}

case class Deeply(inner: SpencerQuery,
                  edgeFilter : Option[EdgeKind] = None) extends SpencerQuery {
  val reachability = edgeFilter match {
      case None => CanReach
      case Some(EdgeKind.FIELD) => CanHeapReach
    }
  val impl = And(List(inner, Not(reachability(Not(inner)))))
  override def explanation(): String = {
    inner.explanation()+", and the same is true for all reachable objects"
  }

  override def getInners = impl.getInners

  override def getSQLBlueprint = {
    impl.getSQLBlueprint
  }

  override def cacheKey: String = super.cacheKey+"_v2" //found a bug, this invalidates old caches

  override def toString = edgeFilter match {
    case None => s"Deeply(${inner.toString})"
    case Some(EdgeKind.FIELD) => s"HeapDeeply(${inner.toString})"
  }

  override def getVersion = { 0 }
}

case class ConstSeq(value: Seq[Long]) extends SpencerQuery {

  override def explanation(): String = "any of "+value.mkString("{", ", ", "}")

  override def getSQLBlueprint = {
    value.mkString("SELECT * FROM (VALUES (", "", s") AS const${this.cacheKey}(id)")
  }

  override def getVersion = { 0 }
}

case class HeapRefersTo(inner: SpencerQuery) extends SpencerQuery {

  override def explanation(): String = "are field-referring to objects that "+inner.explanation()

  override def getInners = List(inner)

  override def getSQLBlueprint = {
    s"""SELECT caller AS id
       |FROM   refs
       |WHERE  kind = 'field'
       |AND    callee IN (
       |  ?
       |)""".stripMargin
  }

  override def getVersion = { 1 }
}

case class RefersTo(inner: SpencerQuery) extends SpencerQuery {

  override def explanation(): String = "are referring to objects that "+inner.explanation()

  override def getInners = List(inner)

  override def getSQLBlueprint = {
    s"""SELECT caller AS id
       |FROM   refs
       |WHERE  callee IN (
       |  ?
       |)""".stripMargin
  }

  override def getVersion = { 1 }
}

case class HeapReferredFrom(inner: SpencerQuery) extends SpencerQuery {

  override def explanation(): String = "are heap-referred to from objects that "+inner.explanation()

  override def getInners = List(inner)

  override def getSQLBlueprint = {
    s"""SELECT callee AS id
       |FROM   refs
       |WHERE  kind = 'field'
       |AND    caller IN (
       |  ?
       |)""".stripMargin
  }

  override def getVersion = { 0 }
}

case class ReferredFrom(inner: SpencerQuery) extends SpencerQuery {

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

  override def getVersion = { 0 }
}

case class HeapReachableFrom(inner: SpencerQuery) extends SpencerQuery {
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

  override def getVersion = { 0 }
}

case class ReachableFrom(inner: SpencerQuery) extends SpencerQuery {
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

  override def getVersion = { 0 }
}

case class CanHeapReach(inner: SpencerQuery) extends SpencerQuery {
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

  override def getVersion = { 0 }
}

case class CanReach(inner: SpencerQuery) extends SpencerQuery {
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

  override def getVersion = { 0 }
}

case class TinyObj() extends SpencerQuery {

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

  override def getVersion = { 0 }
}

object SpencerQueryTest extends App {

  implicit val db: PostgresSpencerDB = new PostgresSpencerDB("test")
  db.connect()

  val watch: Stopwatch = Stopwatch.createStarted()
  val q = QueryParser.parseObjQuery("HeapUniqueObj()").right.get
  println(q.toString)
  println(s"getSQL:\n${q.getSQL}")
  println(s"precacheInnersSQL:\n${q.precacheInnersSQL.mkString("\n")}")
  println(s"getSQLUsingCache:\n${q.getSQLUsingCache}")
  println(s"getCacheSQL: ${q.getCacheSQL}")
  println(s"dependencyTree:\n${q.dependencyTree()}")

  val res = q.analyse //AgeOrderedObj().analyse

  println(
    s"""analysis took ${watch.stop()}
       |getSQL:\n${q.getSQL}
       |precacheInnersSQL:\n${q.precacheInnersSQL.mkString("\n")}
       |getSQLUsingCache:\n${q.getSQLUsingCache}""".stripMargin)

  SQLUtil.print(res)
}

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
  def getCacheSQL: Seq[String] = List()

  def getSQL: Option[String]
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
  override def analyse(implicit g: SpencerDB): DataFrame = {
    val analysed = vs.map(_.analyse)
    analysed.reduce(_.join(_, "id"))
  }

  override def explanation(): String = vs.map(_.explanation()).mkString(", and ")

  override def toString: String = vs.mkString("And(", " ", ")")

//  override def getCacheSQL: Option[Seq[String]] = {
//    inner.
//  }

  override def getSQL: Option[String] = {
    val oqueries = vs.map(_.getSQL)
    if (oqueries.forall(_.isDefined)) {
      val queries = oqueries
        .map(_.get)
      val SQL = queries
        .map(AnaUtil.indent(_,2))
        .mkString("(\n", "\n) INTERSECT (\n", "\n)")
      Some(SQL)
    } else {
      None
    }
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
  override def analyse(implicit g: SpencerDB): DataFrame = {
    if (vs.contains(Obj())) {
      Obj().analyse
    } else {
      vs.map(_.analyse).reduce(_ union _).distinct()
    }
  }

  override def explanation(): String = vs.map(_.explanation()).mkString(", or ")

  override def toString: String = vs.mkString("Or(", " ", ")")

  override def getSQL: Option[String] = {
    val oqueries = vs.map(_.getSQL)
    if (oqueries.forall(_.isDefined)) {
      val queries = oqueries
        .map(_.get)
      val SQL = queries
        .map(AnaUtil.indent(_,2))
        .mkString("(\n", "\n) UNION (\n", "\n)")
      Some(SQL)
    } else {
      None
    }
  }
}

case class SnapshottedVertexIdAnalyser(inner : VertexIdAnalyser) extends VertexIdAnalyser {
  override def analyse(implicit g: SpencerDB): DataFrame = {
    assert(! inner.isInstanceOf[SnapshottedVertexIdAnalyser])
    println(s"analysing snapshotted ${this.toString}")
    val f = () => inner.analyse(g)

    g.getCachedOrDo(inner.toString, f)
    //f()
  }

  override def snapshotted(): VertexIdAnalyser = this

  override def pretty(result: DataFrame): String = inner.pretty(result)

  override def toString: String = inner.toString

  override def explanation(): String = inner.explanation()

  override def getSQL: Option[String] = {
    inner.getSQL
  }
}

/**
  * filters for all objects that only ever have references to objects older than
  * them
  */
case class ReverseAgeOrderedObj() extends VertexIdAnalyser {

  override def analyse(implicit g: SpencerDB): DataFrame = {
    import g.sqlContext.implicits._
    AgeOfNeighbours().snapshotted().analyse
      .groupBy("id", "firstusage")
      .agg(min($"calleefirstusage"), $"firstusage")
      .where($"min(calleefirstusage)" > $"firstusage").select("id") union TinyObj().snapshotted().analyse
  }

  override def explanation(): String = {
    "are only holding field references to objects created after them"
  }

  override def getSQL = {
    Some(s"""SELECT id FROM
            |  (${AnaUtil.indent(AgeOfNeighbours().getSQL.get,3)}) AS AgeOfNeigbhours
            |GROUP BY id, firstusage
            |HAVING MAX(calleefirstusage) > firstusage""".stripMargin)
    }
}

/**
  * filters for all objects that only ever have references to objects younger than
  * them
  */
case class AgeOrderedObj() extends VertexIdAnalyser {
  override def analyse(implicit g: SpencerDB): DataFrame = {
    import g.sqlContext.implicits._
//    AgeOfNeighbours().snapshotted().analyse.show()
    AgeOfNeighbours().snapshotted().analyse
      .groupBy("id", "firstusage")
      .agg(max($"calleefirstusage"), $"firstusage")
      .where($"max(calleefirstusage)" < $"firstusage").select("id") union TinyObj().snapshotted().analyse
  }

  override def explanation(): String = {
    "are only holding field references to objects created before them"
  }

  override def getSQL: Option[String] = {
    Some(s"""SELECT id FROM
            |  (${AnaUtil.indent(AgeOfNeighbours().getSQL.get,3)}) AS AgeOfNeigbhours
            |GROUP BY id, firstusage
            |HAVING MAX(calleefirstusage) < firstusage""".stripMargin)
  }
}

case class AgeOfNeighbours() extends VertexIdAnalyser {
  override def analyse(implicit g: SpencerDB): DataFrame = {
    g.getFrame("refs").createOrReplaceTempView("refs")
    g.selectFrame("objects",getSQL.get)
  }

  override def explanation(): String = "Age"

  override def getSQL: Option[String] = {
    Some("""SELECT
           |  objects.id         AS id,
           |  objects.firstusage AS firstusage,
           |  callees.firstusage AS calleefirstusage
           |FROM objects
           |INNER JOIN refs               ON objects.id = refs.caller
           |INNER JOIN objects AS callees ON refs.callee = callees.id
           |WHERE
           |  refs.kind = 'field'""".stripMargin)
  }
}

case class MutableObj() extends VertexIdAnalyser {

  override def analyse(implicit g: SpencerDB): DataFrame = {
    val ret = g.selectFrame("uses_cstore", getSQL.get)
      .withColumnRenamed("callee", "id")
      .repartition(1000)
    println(s"partitions in ${this.toString}: ${ret.rdd.partitions.size}")
    ret
  }

  override def explanation(): String = "are changed outside their constructor"

  override def getSQL: Option[String] = {
    Some("""SELECT DISTINCT callee AS id
           |FROM uses_cstore
           |WHERE
           |  callee > 4 AND
           |  NOT(caller = callee AND method = '<init>') AND
           |  (kind = 'fieldstore' OR kind = 'modify')""".stripMargin)
  }
}

object ThreadLocalObj {
  def apply() : VertexIdAnalyser = {
    Named(IsNot(NonThreadLocalObj()), "ThreadLocalObj()", "are accessed by only one thread")
  }
}

case class NonThreadLocalObj() extends VertexIdAnalyser {
  override def analyse(implicit g: SpencerDB): DataFrame = {
    g.selectFrame("uses_cstore", getSQL.get)
      .withColumnRenamed("callee", "id")
  }

  override def explanation(): String = "are changed outside their constructor"

  override def getSQL: Option[String] = {
    Some("""SELECT callee
           |FROM uses_cstore
           |WHERE callee > 4
           |GROUP BY callee
           |HAVING COUNT(DISTINCT thread) > 1
           |""".stripMargin)
  }
}

case class UniqueObj() extends VertexIdAnalyser {
  override def getSQL: Option[String] = {
    Some("""SELECT callee FROM
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
           |HAVING MAX(sum_at_time) = 1""".stripMargin)
  }

  override def analyse(implicit g: SpencerDB): DataFrame = {
    MaxInDegree.UniqueObj().analyse
  }

  override def explanation(): String = "are never aliased"
}

case class HeapUniqueObj() extends VertexIdAnalyser {
  override def getSQL: Option[String] = {
    Some("""SELECT callee FROM
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
           |HAVING MAX(sum_at_time) = 1""".stripMargin)
  }

  override def analyse(implicit g: SpencerDB): DataFrame = {
    MaxInDegree.HeapUniqueObj().analyse
  }

  override def explanation(): String = "are never aliased"
}

case class StackBoundObj() extends VertexIdAnalyser {
  override def getSQL: Option[String] = {
    Some("""SELECT id FROM objects EXCEPT (SELECT callee FROM
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
           |HAVING MAX(sum_at_time) > 0)""".stripMargin)
  }

  override def analyse(implicit g: SpencerDB): DataFrame = {
    MaxInDegree.StackBoundObj().analyse
  }

  override def explanation(): String = "are never aliased"
}

case class ImmutableObj() extends VertexIdAnalyser {

  override def analyse(implicit g: SpencerDB): DataFrame = {
    print("ImmutableObj: analyse Obj..")
    val objects = Obj().snapshotted().analyse.select("id")
    println("done")

    print("ImmutableObj: analyse MutableObj.. analyse Obj().except..")
    val immutableIDs = objects.except(MutableObj().snapshotted().analyse.select("id"))
    println("done")
    print("ImmutableObj: join..")
    val res = objects.join(immutableIDs, usingColumn = "id")
    println("done")
    res
  }

  override def explanation(): String = "are never changed outside their constructor"

  override def getSQL: Option[String] = {
    IsNot(MutableObj().snapshotted()).getSQL
  }
}

case class StationaryObj() extends VertexIdAnalyser {
  override def analyse(implicit g: SpencerDB): DataFrame = {
    import g.sqlContext.implicits._
    val firstReads = g.selectFrame("uses_cstore",
      """SELECT
        |  callee, MIN(idx)
        |FROM
        |  uses_cstore
        |WHERE
        |  callee > 4 AND
        |  method != '<init>' AND
        |  (kind = 'fieldload' OR kind = 'read')
        |GROUP BY callee
        |""".stripMargin).withColumnRenamed("min(idx)", "firstRead")

    val lastWrites = g.selectFrame("uses_cstore",
        """SELECT
          |  callee, MAX(idx)
          |FROM
          |  uses_cstore
          |WHERE
          |  callee > 4 AND
          |  method != '<init>' AND
          |  (kind = 'fieldstore' OR kind = 'modify')
          |GROUP BY callee
          |""".stripMargin).withColumnRenamed("max(idx)", "lastWrite")

    val joined = firstReads.join(lastWrites, "callee").withColumnRenamed("callee", "id")
    val writeAfterRead = joined.filter($"lastWrite" > $"firstRead")

    Obj().snapshotted().analyse.select("id").join(writeAfterRead, List("id"), "left_anti")
  }

  override def explanation(): String = "are never changed after being read from for the first time"

  override def getSQL: Option[String] = {
    Some("""SELECT id FROM objects
           |EXCEPT
           |(SELECT reads.callee id FROM
           |(
           |  SELECT
           |    callee, MIN(idx)
           |  FROM
           |    uses_cstore
           |  WHERE
           |    callee > 4 AND
           |    method != '<init>' AND
           |    (kind = 'fieldload' OR kind = 'read')
           |  GROUP BY callee
           |) reads
           |FULL OUTER JOIN
           |(
           |  SELECT
           |    callee, MAX(idx)
           |  FROM
           |    uses_cstore
           |  WHERE
           |    callee > 4 AND
           |    method != '<init>' AND
           |    (kind = 'fieldstore' OR kind = 'modify')
           |  GROUP BY callee
           |) writes
           |ON reads.callee = writes.callee
           |WHERE writes.max > reads.min)
           |""".stripMargin)
  }
}

case class Obj() extends VertexIdAnalyser {

  override def analyse(implicit g: SpencerDB): DataFrame = {
    g.selectFrame("objects", getSQL.get)
  }

  override def explanation(): String = "were traced"

  override def getSQL: Option[String] = {
    Some("SELECT id FROM objects WHERE id >= 4")
  }
}

case class AllocatedAt(allocationSite: (String, Long)) extends VertexIdAnalyser {

  override def analyse(implicit g: SpencerDB): DataFrame = {
    g.selectFrame("objects", getSQL.get)
  }

  override def toString: String = {
    "AllocatedAt("+allocationSite._1+":"+allocationSite._2.toString+")"
  }

  override def explanation(): String = "were allocated at "+allocationSite._1+":"+allocationSite._2

  override def getSQL: Option[String] = {
    Some(s"""SELECT id FROM objects WHERE
            |allocationsitefile = '${allocationSite._1}' AND
            |allocationsiteline = ${allocationSite._2}""".stripMargin)
  }
}

case class InstanceOf(klassName: String) extends VertexIdAnalyser {

  def this(klass: Class[_]) =
    this(klass.getName)

  override def analyse(implicit g: SpencerDB): DataFrame = {
    g.selectFrame("objects", getSQL.get)
  }

  override def explanation(): String = "are instances of class "+klassName

  override def getSQL: Option[String] = {
    Some(s"""SELECT id FROM objects WHERE klass = '$klassName'""".stripMargin)
  }
}

case class IsNot(inner: VertexIdAnalyser) extends VertexIdAnalyser {
  override def analyse(implicit g: SpencerDB): DataFrame = {
    Obj().snapshotted().analyse.join(inner.analyse, List("id"), "left_anti")
  }

  override def explanation(): String = "not "+inner.explanation()

  override def getSQL: Option[String] = {
    inner.getSQL.map(sql =>
      s"""SELECT id FROM objects WHERE id > 4
         |EXCEPT
         |  (${AnaUtil.indent(sql,3)})
        """.stripMargin)
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

  override def analyse(implicit g: SpencerDB): DataFrame = inner.analyse

  override def pretty(result: DataFrame): String = inner.pretty(result)

  override def toString: String = name

  override def explanation(): String = this.expl

  override def getSQL: Option[String] = {
    inner.getSQL
  }
}

case class Deeply(inner: VertexIdAnalyser,
                  edgeFilter : Option[EdgeKind => Boolean] = None) extends VertexIdAnalyser {
  override def analyse(implicit g: SpencerDB): DataFrame = {
    val allObjs = Obj().analyse
    val negativeRoots = allObjs.join(inner.analyse, List("id"), "left_anti")
    val reachingNegativeRoots = ConnectedWith(Const(negativeRoots), reverse = true, edgeFilter)
    IsNot(reachingNegativeRoots).analyse
  }

  override def explanation(): String = {
    inner.explanation()+", and the same is true for all reachable objects"
  }

  override def getSQL: Option[String] = {
    None //FIXME
  }
}

case class ConstSeq(value: Seq[VertexId]) extends VertexIdAnalyser {
  override def analyse(implicit g: SpencerDB): DataFrame = {
    import g.sqlContext.implicits._
    g.sqlContext.sparkSession.sparkContext.parallelize(value).toDF().withColumnRenamed("value", "id")
  }

  override def pretty(result: DataFrame): String = {
    value.mkString("[ ", ", ", " ]")
  }

  override def explanation(): String = "any of "+value.mkString("{", ", ", "}")

  override def getSQL: Option[String] = {
    Some(value.mkString("SELECT * FROM (VALUES (", "", s") AS const${Math.abs(this.toString.hashCode)}(id)"))
  }
}

case class Const(value: DataFrame) extends VertexIdAnalyser {
  override def analyse(implicit g: SpencerDB):DataFrame = value

  override def pretty(result: DataFrame): String = this.toString

  override def explanation(): String = "constant set "+value.toString

  override def getSQL: Option[String] = {
    None //FIXME
  }
}

case class HeapRefersTo(inner: VertexIdAnalyser) extends VertexIdAnalyser {
  override def analyse(implicit g: SpencerDB): DataFrame = {
    val roots = inner.analyse
    g
      .selectFrame("refs", "SELECT caller, callee AS id FROM refs WHERE kind = 'field'")
      .join(roots, List("id"), "leftsemi")
      .select("caller")
      .withColumnRenamed("caller", "id")
  }

  override def explanation(): String = "are field-referring to objects that "+inner.explanation()

  override def getSQL: Option[String] = {
    inner.getSQL.map(sql =>
      s"""SELECT
         |  callee AS id
         |FROM
         |  refs
         |WHERE
         |  kind = 'field' AND
         |  callee IN (
         |    ${AnaUtil.indent(sql,4)}
         |  )""".stripMargin)
  }
}

case class RefersTo(inner: VertexIdAnalyser) extends VertexIdAnalyser {
  override def analyse(implicit g: SpencerDB): DataFrame = {
    val roots = inner.analyse
    g
      .selectFrame("refs", "SELECT caller, callee AS id FROM refs")
      .join(roots, List("id"), "leftsemi")
      .select("caller")
      .withColumnRenamed("caller", "id")
  }

  override def explanation(): String = "are referring to objects that "+inner.explanation()

  override def getSQL: Option[String] = {
    inner.getSQL.map(sql =>
      s"""SELECT
         |  callee AS id
         |FROM
         |  refs
         |WHERE
         |  callee IN (
         |    ${AnaUtil.indent(sql,4)}
         |  )""".stripMargin)
  }
}

case class HeapReferredFrom(inner: VertexIdAnalyser) extends VertexIdAnalyser {
  override def analyse(implicit g: SpencerDB): DataFrame = {
    val roots = inner.analyse
    g
      .selectFrame("refs", "SELECT caller, callee FROM refs WHERE kind = 'field'")
      .withColumnRenamed("caller", "id")
      .join(roots, List("id"), "leftsemi")
      .select("callee")
      .withColumnRenamed("callee", "id")
  }

  override def explanation(): String = "are heap-referred to from objects that "+inner.explanation()

  override def getSQL: Option[String] = {
    inner.getSQL.map(sql =>
      s"""SELECT
         |  callee AS id
         |FROM
         |  refs
         |WHERE
         |  kind = 'field' AND
         |  caller IN (
         |    ${AnaUtil.indent(sql,4)}
         |  )""".stripMargin)
  }
}

case class ReferredFrom(inner: VertexIdAnalyser) extends VertexIdAnalyser {
  override def analyse(implicit g: SpencerDB): DataFrame = {
    val roots = inner.analyse
    g
      .selectFrame("refs", "SELECT caller, callee FROM refs")
      .withColumnRenamed("caller", "id")
      .join(roots, List("id"), "leftsemi")
      .select("callee")
      .withColumnRenamed("callee", "id")
  }

  override def explanation(): String = "are referred to from objects that "+inner.explanation()

  override def getSQL: Option[String] = {
    inner.getSQL.map(sql =>
      s"""SELECT
         |  callee AS id
         |FROM
         |  refs
         |WHERE
         |  caller IN (
         |    ${AnaUtil.indent(sql,4)}
         |  )""".stripMargin)
  }
}

case class HeapReachableFrom(inner: VertexIdAnalyser) extends VertexIdAnalyser {
  override def getSQL: Option[String] = {
    inner.getSQL.map(sql =>
      s"""WITH RECURSIVE heapreachablefrom(id) AS (
         |    ${AnaUtil.indent(sql,4)}
         |  UNION
         |    SELECT
         |      refs.callee AS id
         |    FROM refs
         |    JOIN heapreachablefrom ON heapreachablefrom.id = refs.caller
         |    WHERE kind = 'field'
         |)
         |SELECT id FROM heapreachablefrom""".stripMargin)
  }

  override def analyse(implicit g: SpencerDB): DataFrame = {
    ConnectedWith(inner, edgeFilter = Some(_ == EdgeKind.FIELD)).analyse
  }

  override def explanation(): String = s"are heap-reachable from objects that ${inner.explanation()}"
}

case class ReachableFrom(inner: VertexIdAnalyser) extends VertexIdAnalyser {
  override def getSQL: Option[String] = {
    inner.getSQL.map(sql =>
      s"""WITH RECURSIVE reachablefrom(id) AS (
         |    ${AnaUtil.indent(sql,4)}
         |  UNION
         |    SELECT
         |      refs.callee AS id
         |    FROM refs
         |    JOIN reachablefrom ON reachablefrom.id = refs.caller
         |)
         |SELECT id FROM reachablefrom""".stripMargin)
  }

  override def analyse(implicit g: SpencerDB): DataFrame = {
    ConnectedWith(inner).analyse
  }

  override def explanation(): String = s"are reachable from objects that ${inner.explanation()}"
}

case class CanHeapReach(inner: VertexIdAnalyser) extends VertexIdAnalyser {
  override def getSQL: Option[String] = {
    inner.getSQL.map(sql =>
      s"""WITH RECURSIVE canheapreach(id) AS (
         |    ${AnaUtil.indent(sql,4)}
         |  UNION
         |    SELECT
         |      refs.caller AS id
         |    FROM refs
         |    JOIN canheapreach ON canheapreach.id = refs.callee
         |    WHERE kind = 'field'
         |)
         |SELECT id FROM canheapreach""".stripMargin)
  }

  override def analyse(implicit g: SpencerDB): DataFrame = {
    ConnectedWith(inner, edgeFilter = Some(_ == EdgeKind.FIELD), reverse = true).analyse
  }

  override def explanation(): String = s"are able to heap-reach objects that ${inner.explanation()}"
}

case class CanReach(inner: VertexIdAnalyser) extends VertexIdAnalyser {
  override def getSQL: Option[String] = {
    inner.getSQL.map(sql =>
      s"""WITH RECURSIVE canreach(id) AS (
         |    ${AnaUtil.indent(sql, 4)}
         |  UNION
         |    SELECT
         |      refs.caller AS id
         |    FROM refs
         |    JOIN canreach ON canreach.id = refs.callee
         |)
         |SELECT id FROM canreach""".stripMargin)
  }

  override def analyse(implicit g: SpencerDB): DataFrame = {
    ConnectedWith(inner, reverse = true).analyse
  }

  override def explanation(): String = s"are able to reach objects that ${inner.explanation()}"
}

case class ConnectedWith(roots: VertexIdAnalyser
                         , reverse : Boolean = false
                         , edgeFilter : Option[EdgeKind => Boolean] = None) extends VertexIdAnalyser {

  override def analyse(implicit g: SpencerDB): DataFrame = {
    import g.sqlContext.implicits._
    val rootsAnalysed = roots.analyse
    val rootsCollected = rootsAnalysed.select("id").as[Long].collect().toSet

    val empty = Set().asInstanceOf[Set[VertexId]]
    val mappedGraph = g.getGraph()
      .mapVertices { case (vertexId, objDesc) =>

        rootsCollected.contains(vertexId.asInstanceOf[Long])
      }

    val subgraph = edgeFilter match {
      case Some(epred) => mappedGraph.subgraph(epred = triplet => epred(triplet.attr.kind))
      case None => mappedGraph
    }

    val directionCorrectedGraph = if (reverse) subgraph.reverse else subgraph

    val computed = directionCorrectedGraph
      .pregel(
        initialMsg = false
      )(
        vprog = {
          case (vertexId, isReached, reachedNow) =>
            isReached || reachedNow
        },
        sendMsg =  triplet => {
          if (!triplet.dstAttr && triplet.srcAttr) {
            Iterator((triplet.dstId, true))
          } else {
            Iterator.empty
          }
        },
        mergeMsg = _ && _)
    computed
      .vertices
      .filter(_._2)
      .map(_._1).toDF.withColumnRenamed("value", "id")
  }

  override def explanation(): String = if (reverse) {
    "can reach any objects that "+roots.explanation()
  } else {
    "are reachable from objects that "+roots.explanation()
  }

  override def getSQL: Option[String] = {
    None //FIXME
  }
}

case class TinyObj() extends VertexIdAnalyser {
  override def analyse(implicit g: SpencerDB): DataFrame = {
    val withRefTypeFields = g.selectFrame("refs", "SELECT DISTINCT caller FROM refs WHERE kind = 'field'").withColumnRenamed("caller", "id")
    Obj().snapshotted().analyse.join(withRefTypeFields, List("id"), "left_anti")
  }

  override def explanation(): String = "do not have or do not use reference type fields"

  override def getSQL: Option[String] = {
    Some(
      s"""(${AnaUtil.indent(Obj().snapshotted().getSQL.get,1)})
        |  EXCEPT
        |  (SELECT
        |     DISTINCT caller
        |   FROM
        |     refs
        |   WHERE
        |     kind = 'field')""".stripMargin)
  }
}

object VertexIdAnalyserTest extends App {

  implicit val db: SpencerDB = new PostgresSpencerDB("test")
  db.connect()

  val watch: Stopwatch = Stopwatch.createStarted()
  val q = StackBoundObj()
  println(s"getSQL:\n${q.getSQL.getOrElse("<none>")}")
  val res = q.analyse //AgeOrderedObj().analyse

  res.repartition()
  res.show()
  println(
    s"""analysis took ${watch.stop()}
       |got ${res.count} objects
       |getSQL:\n${q.getSQL.getOrElse("<none>")}
     """.stripMargin)
}

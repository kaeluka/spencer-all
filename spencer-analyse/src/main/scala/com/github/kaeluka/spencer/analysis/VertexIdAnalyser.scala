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
}

object Or {
  def apply(vs_ : Seq[VertexIdAnalyser]) : VertexIdAnalyser = {
    vs_.find(_.toString == "Obj()") match {
      case Some(q) => q
      case None =>
        val vs = vs_
          .flatMap({
            case Or_(innerVs) => innerVs
            case other => List(other)
          })
        Or_(vs)
    }
  }
}

case class Or_(vs: Seq[VertexIdAnalyser]) extends VertexIdAnalyser {
  override def analyse(implicit g: SpencerDB): DataFrame = {
    if (vs.contains(Obj())) {
      Obj().analyse
    } else {
      vs.map(_.analyse).reduce(_ union _).distinct()
    }
  }

  override def explanation(): String = vs.map(_.explanation()).mkString(", or ")

  override def toString: String = vs.mkString("Or(", " ", ")")
}

case class SnapshottedVertexIdAnalyser(inner : VertexIdAnalyser) extends VertexIdAnalyser {
  override def analyse(implicit g: SpencerDB): DataFrame = {
    println(s"analysing snapshotted ${this.toString}, inner class: ${inner.getClass.getName}")
    assert(! inner.isInstanceOf[SnapshottedVertexIdAnalyser])
    val tblName = ("cache_"+ inner.toString.hashCode.toString+"_"+inner.getClass.getName.toString.hashCode).replace("-", "_")
    val f = () => inner.analyse(g)

    g.getCachedOrDo(tblName, f)
    //f()
  }

  override def snapshotted(): VertexIdAnalyser = this

  override def pretty(result: DataFrame): String = inner.pretty(result)

  override def toString: String = inner.toString

  override def explanation(): String = inner.explanation()
}

/**
  * filters for all objects that only ever have references to objects older than
  * them
  */
case class ReverseAgeOrderedObj() extends VertexIdAnalyser {

  override def analyse(implicit g: SpencerDB): DataFrame = {
    import g.sqlContext.implicits._
    AgeOfNeighbours().analyse
      .groupBy("id", "firstUsage")
      .agg(min($"calleeFirstUsage"), $"firstUsage")
      .where($"min(calleeFirstUsage)" > $"firstUsage").select("id") union TinyObj().snapshotted().analyse
  }

  override def explanation(): String = {
    "are only holding field references to objects created after them"
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
    AgeOfNeighbours().analyse
      .groupBy("id", "firstUsage")
      .agg(max($"calleeFirstUsage"), $"firstUsage")
      .where($"max(calleeFirstUsage)" < $"firstUsage").select("id") union TinyObj().snapshotted().analyse
  }

  override def explanation(): String = {
    "are only holding field references to objects created before them"
  }
}

case class AgeOfNeighbours() extends VertexIdAnalyser {
  override def analyse(implicit g: SpencerDB): DataFrame = {
    g.getFrame("refs").createOrReplaceTempView("refs")
    val query =
      """SELECT
        |  objects.id         AS id,
        |  objects.firstUsage AS firstUsage,
        |  callees.firstUsage AS calleeFirstUsage
        |FROM objects
        |INNER JOIN refs               ON objects.id = refs.caller
        |INNER JOIN objects AS callees ON refs.callee = callees.id
        |WHERE
        |  refs.kind = 'field'
      """.stripMargin

    g.selectFrame("objects", query)
  }

  override def explanation(): String = "Age"
}


case class MutableObj() extends VertexIdAnalyser {

  override def analyse(implicit g: SpencerDB): DataFrame = {
    val objects = Obj().analyse

    g.selectFrame("uses",
        """SELECT DISTINCT callee
          |FROM uses
          |WHERE
          |  callee > 4 AND
          |  method != '<init>' AND
          |  (kind = 'fieldstore' OR kind = 'modify')""".
      stripMargin)
        .withColumnRenamed("callee", "id")
  }

  override def explanation(): String = "are changed outside their constructor"
}

case class ImmutableObj() extends VertexIdAnalyser {

  override def analyse(implicit g: SpencerDB): DataFrame = {
    val objects = Obj().analyse.select("id")

    val immutableIDs = objects.except(MutableObj().analyse.select("id"))
    objects.join(immutableIDs, usingColumn = "id").distinct()
  }

  override def explanation(): String = "are never changed outside their constructor"
}

case class StationaryObj() extends VertexIdAnalyser {
  def analyse_old(implicit g: SpencerDB): DataFrame = {
    import g.sqlContext.implicits._
    val writeAfterRead = g.selectFrame("uses", "SELECT callee, method, kind FROM uses WHERE callee > 4 AND method != '<init>'")
      .as[(Long, String, String)]
      .rdd
      .groupBy(_._1)
      .filter({
        case (callee, events) =>
          var hadRead = false
          var res : Option[VertexId] = Some(callee)
          val it: Iterator[(Long, String, String)] = events.iterator
          while (it.hasNext && res.nonEmpty) {
            val nxt = it.next()
            val methd = nxt._2
            val kind = nxt._3
            if (hadRead) {
              if (kind == "fieldstore" || kind == "modify") {
                res = None
              }
            } else {
              if (kind == "fieldload" || kind == "read") {
                hadRead = true
              }
            }
          }
          res.isEmpty
      })
      .map(_._1)

    Obj().analyse.select("id").as[Long].rdd.subtract(writeAfterRead).toDF.withColumnRenamed("value", "id")
  }

  override def analyse(implicit g: SpencerDB): DataFrame = {
    import g.sqlContext.implicits._
    val firstReads = g.selectFrame("uses",
      """SELECT
        |  callee, MIN(idx)
        |FROM
        |  uses
        |WHERE
        |  callee > 4 AND
        |  method != '<init>' AND
        |  (kind = 'fieldload' OR kind = 'read')
        |GROUP BY callee
        |""".stripMargin).withColumnRenamed("min(idx)", "firstRead")

    val lastWrites = g.selectFrame("uses",
        """SELECT
          |  callee, MAX(idx)
          |FROM
          |  uses
          |WHERE
          |  callee > 4 AND
          |  method != '<init>' AND
          |  (kind = 'fieldstore' OR kind = 'modify')
          |GROUP BY callee
          |""".stripMargin).withColumnRenamed("max(idx)", "lastWrite")

    val joined = firstReads.join(lastWrites, "callee").withColumnRenamed("callee", "id")
    val writeAfterRead = joined.filter($"lastWrite" > $"firstRead")

    Obj().analyse.select("id").join(writeAfterRead, List("id"), "left_anti")
  }

  override def explanation(): String = "are never changed after being read from for the first time"
}

case class Obj() extends VertexIdAnalyser {

  override def analyse(implicit g: SpencerDB): DataFrame = {
    g.selectFrame("objects", "SELECT id FROM objects WHERE id >= 4")
  }

  override def explanation(): String = "were traced"
}

case class AllocatedAt(allocationSite: (String, Long)) extends VertexIdAnalyser {

  override def analyse(implicit g: SpencerDB): DataFrame = {
    g.selectFrame("objects", s"SELECT id FROM objects WHERE " +
      s"allocationsitefile = '${allocationSite._1}' AND " +
      s"allocationsiteline = ${allocationSite._2}")
  }

  override def toString: String = {
    "AllocatedAt("+allocationSite._1+":"+allocationSite._2.toString+")"
  }

  override def explanation(): String = "were allocated at "+allocationSite._1+":"+allocationSite._2
}

case class InstanceOfClass(klassName: String) extends VertexIdAnalyser {

  def this(klass: Class[_]) =
    this(klass.getName)

  override def analyse(implicit g: SpencerDB): DataFrame = {
    g.selectFrame("objects", s"SELECT id FROM objects WHERE klass = '$klassName'")
  }

  override def explanation(): String = "are instances of class "+klassName
}

case class IsNot(inner: VertexIdAnalyser) extends VertexIdAnalyser {
  override def analyse(implicit g: SpencerDB): DataFrame = {
    Obj().analyse.join(inner.analyse, List("id"), "left_anti")
  }

  override def explanation(): String = "not "+inner.explanation()
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
}

case class Deeply(inner: VertexIdAnalyser) extends VertexIdAnalyser {
  override def analyse(implicit g: SpencerDB): DataFrame = {
    val allObjs = Obj().analyse
    val negativeRoots = allObjs.join(inner.analyse, List("id"), "left_anti")
    val reachingNegativeRoots = ConnectedWith(Const(negativeRoots), reverse = true, edgeFilter = Some(_ == EdgeKind.FIELD))
    IsNot(reachingNegativeRoots).analyse
  }

  override def explanation(): String = {
    inner.explanation()+", and the same is true for all reachable objects"
  }
}

case class ConstSeq(value: Seq[VertexId]) extends VertexIdAnalyser {
  override def analyse(implicit g: SpencerDB): DataFrame = {
    ???
//    g.sqlContext.sparkSession.sparkContext.parallelize(value).toDF()
  }

  override def pretty(result: DataFrame): String = {
    value.mkString("[ ", ", ", " ]")
  }

  override def explanation(): String = "any of "+value.mkString("{", ", ", "}")
}

case class Const(value: DataFrame) extends VertexIdAnalyser {
  override def analyse(implicit g: SpencerDB):DataFrame = value

  override def pretty(result: DataFrame): String = this.toString

  override def explanation(): String = "constant set "+value.toString
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
}

case class TinyObj() extends VertexIdAnalyser {
  override def analyse(implicit g: SpencerDB): DataFrame = {
    val withRefTypeFields = g.selectFrame("refs", "SELECT DISTINCT caller FROM refs WHERE kind = 'field'").withColumnRenamed("caller", "id")
    Obj().analyse.join(withRefTypeFields, List("id"), "left_anti")
  }

  override def explanation(): String = "do not have or do not use reference type fields"
}

object VertexIdAnalyserTest extends App {

  implicit val db: SpencerDB = new PostgresSpencerDB("test")
  db.connect()

  val watch: Stopwatch = Stopwatch.createStarted()
  val res = AgeOrderedObj().analyse //AgeOrderedObj().analyse
  res.repartition().cache().show()
  println("analysis took "+watch.stop())
  println(s"got ${res.count} objects")
}

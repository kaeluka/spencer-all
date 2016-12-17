package com.github.kaeluka.spencer.analysis

import com.github.kaeluka.spencer.PostgresSpencerDB
import com.github.kaeluka.spencer.analysis.EdgeKind.EdgeKind
import com.google.common.base.Stopwatch
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

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

case class MutableObj() extends VertexIdAnalyser {

  override def analyse(implicit g: SpencerDB): DataFrame = {
    import g.sqlContext.implicits._
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
    import g.sqlContext.implicits._
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
    import g.sqlContext.implicits._
    g.selectFrame("objects", "SELECT id FROM objects WHERE id >= 4")
  }

  override def explanation(): String = "were traced"
}

case class AllocatedAt(allocationSite: (String, Long)) extends VertexIdAnalyser {

  override def analyse(implicit g: SpencerDB): DataFrame = {
    import g.sqlContext.implicits._
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
    import g.sqlContext.implicits._
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

//case class ObjWithInstanceCountAtLeast(n : Int) extends VertexIdAnalyser {
//  override def analyse(implicit g: SpencerDB): DataFrame = {
//    ObjsByClass().analyse
//      .filter(_._2.size >= n)
//      .flatMap(_._2)
//  }
//
//  override def explanation(): String = "created from classes with at least "+n+" instances in total"
//}

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
        import g.sqlContext.implicits._
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
    import g.sqlContext.implicits._
    val withRefTypeFields = g.selectFrame("refs", "SELECT DISTINCT caller FROM refs WHERE kind = 'field'").withColumnRenamed("caller", "id")
    Obj().analyse.join(withRefTypeFields, List("id"), "left_anti")
  }

  override def explanation(): String = "do not have or do not use reference type fields"
}

/*
case class GroupByAllocationSite(inner: VertexIdAnalyser) extends SpencerAnalyser[RDD[((Option[String], Option[Long]), Iterable[VertexId])]] {
  override def analyse(implicit g: SpencerDB): RDD[((Option[String], Option[Long]), Iterable[VertexId])] = {
    val innerCollected = inner.analyse.collect()
    g.db.getTable("objects")
      .select("allocationsitefile", "allocationsiteline", "id")
      .where("id IN ?", innerCollected.toList)
      .map(row => (row.getStringOption("allocationsitefile"), row.getLongOption("allocationsiteline"), row.getLong("id")))
      .map({case (row, line, id) =>
        (
          (
            row match {
              case Some(r) =>
                if (r.startsWith("<"))
                  None
                else
                  Some(r)
              case other => other
            },
            line match {
              case Some(-1) => None
              case other => other
            })
          , id
          )
      })
      .groupBy(_._1)
      .map({case (klass, iter) => (klass, iter.map(_._2))})
  }

  override def pretty(result: RDD[((Option[String], Option[Long]), Iterable[VertexId])]): String = {
    val collected =
      (if (result.count() < 1000) {
        result.sortBy(_._2.size*(-1))
      } else {
        result
      }).collect()

    this.toString+":\n"+
      collected.map({
        case (allocationSite, instances) =>
          val size = instances.size
          allocationSite+"\t-\t"+(if (size > 50) {
            instances.take(50).mkString("\t"+size+" x - [ ", ", ", " ... ]")
          } else {
            instances.mkString("\t"+size+" x - [ ", ", ", " ]")
          })
      }).mkString("\n")
  }

  override def explanation(): String = "some objects, grouped by allocation site"
}
*/

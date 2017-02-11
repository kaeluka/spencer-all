package com.github.kaeluka.spencer.analysis
import com.datastax.spark.connector.CassandraRow
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import org.apache.spark.graphx.{VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

object InDegreeSpec extends Enumeration {
  type InDegreeSpec = Value
  val HEAP, STACK, HEAP_OR_STACK = Value
}

import InDegreeSpec._

case class NamedFunc[T,U](p: T => U, name: String) extends Function[T, U] {
  override def apply(x: T): U = p(x)

  override def toString(): String = name
}

object MaxInDegree {
  val Aliased : Function[Int, Boolean] = NamedFunc(_ > 1, "Aliased")
  val Unique : Function[Int, Boolean] = NamedFunc(_ == 1, "Unique")
  val None : Function[Int, Boolean] = NamedFunc(_ == 0,   "None")

  def UniqueObj() : VertexIdAnalyser = {
    Named(MaxInDegree(Unique, "UniqueObj()", InDegreeSpec.HEAP_OR_STACK), "UniqueObj()", "are never aliased")
  }

  def HeapUniqueObj() : VertexIdAnalyser = {
    Named(MaxInDegree(Unique, "HeapUniqueObj()", InDegreeSpec.HEAP), "HeapUniqueObj()", "are reachable from the heap but only from one alias")
  }

  def StackBoundObj() : VertexIdAnalyser = {
    Named(MaxInDegree(None, "StackBoundObj()", InDegreeSpec.HEAP), "StackBoundObj()", "are only used from the stack")
  }

  def highestOverlap(edges : Seq[(Option[Long], Option[Long])]) : Int = {
//    println(edges.mkString(", "))
    var maxT : Long = 0
    for ((s, e) <- edges) {
      if (s.getOrElse(0.asInstanceOf[Long]) > maxT) {
        maxT = s.get
      }
      if (e.getOrElse(0.asInstanceOf[Long]) > maxT) {
        maxT = e.get
      }
    }

    val starts = edges.map(x => (x._1.getOrElse(0.asInstanceOf[Long]),  1))
    val ends   = edges.map(x => (x._2.getOrElse(maxT+1)              , -1))

    val merged = (starts ++ ends).sorted

    var depth = 0
    var maxDepth = 0
    for ((_, diff) <- merged) {
      depth = depth+diff
      if (maxDepth < depth) {
        maxDepth =depth
      }
    }
    maxDepth
  }
}

case class InRefsHistory(spec: InDegreeSpec = HEAP_OR_STACK) extends SpencerAnalyser[RDD[(VertexId, Seq[(Long, Array[VertexId])])]] {
  case class InRef(callee: Long, start: Long, end: Option[Long], holder: VertexId)

  override def analyse(implicit g: SpencerDB): RDD[(VertexId, Seq[(Long, Array[VertexId])])] = {
    import g.sqlContext.implicits._
    val refs = (spec match {
      case HEAP =>          g.selectFrame("refs", "SELECT callee, refstart, refend, caller FROM refs WHERE kind = 'field'")
      case STACK =>         g.selectFrame("refs", "SELECT callee, refstart, refend, caller FROM refs WHERE kind = 'var'")
      case HEAP_OR_STACK => g.selectFrame("refs", "SELECT callee, refstart, refend, caller FROM refs")
    }).as[(Long, Long, Long, Long)].rdd

    //FIXME dataframe ops might be much better at grouping
    val res = refs
      .map(row => InRef(callee = row._1, start = row._2, end = Some(row._3), holder = row._4))
      .groupBy(_.callee)
      .map {
        case (callee, edges) =>
          val starts = edges.map(x => (x.start, x.holder, true))
          val ends   = edges.filter(_.end.nonEmpty).map(x => (x.end.get, x.holder, false))

          var sum = 0
          var res = new mutable.ArrayBuffer[(Long, Array[VertexId])]
          var set = Set[VertexId]()
          for ((idx, holder, wasAdded) <- (starts ++ ends).toSeq.sortBy(_._1)) {
            if (wasAdded) {
              set = set + holder
            } else {
              set = set - holder
            }
            res.append((idx, set.toArray))
          }
          (callee, res.toSeq)
      }
    res
  }

  override def pretty(result: RDD[(VertexId, Seq[(Long, Array[VertexId])])]): String = {
    val c = result.collect().toMap
    c.iterator.map {
      case (k, v) =>
        k + " - " + v.map(t_holders => t_holders._1+": "+t_holders._2.mkString("{", " ", "}")).mkString(" | ")
    }.mkString("\n")
  }

  override def explanation(): String = "the in-reference history of an object"
}

case class InDegreeMap(spec: InDegreeSpec = HEAP_OR_STACK) extends SpencerAnalyser[RDD[(VertexId, Seq[(Long, Int)])]] {
  override def analyse(implicit g: SpencerDB): RDD[(VertexId, Seq[(VertexId, Int)])] = {
    import g.sqlContext.implicits._
    val refs = (spec match {
//      case HEAP => g.db.getTable("refs").filter(_.getString("kind") == "field")
//      case STACK => g.db.getTable("refs").filter(_.getString("kind") == "var")
//      case HEAP_OR_STACK => g.db.getTable("refs")
      case HEAP => g.selectFrame("refs",          "SELECT callee, refstart, refend FROM refs WHERE kind = 'field'")
      case STACK => g.selectFrame("refs",         "SELECT callee, refstart, refend FROM refs WHERE kind = 'var'")
      case HEAP_OR_STACK => g.selectFrame("refs", "SELECT callee, refstart, refend FROM refs")
    }).as[(Long, Long, Option[Long])].rdd

    val res = refs
      .map(row => (row._1.asInstanceOf[VertexId], row._2, row._3))
      .groupBy(_._1)
      .map({
        case (callee, edges) => {
          val starts = edges.map(x => (x._2, 1))
          val ends   = edges.filter(_._3.nonEmpty).map(x => (x._3.get, -1))
          var sum = 0
          var res = new mutable.ArrayBuffer[(Long, Int)]
          for ((idx, diff) <- (starts ++ ends).toSeq.sorted) {
            sum = sum+diff
            res.append((idx, sum))
          }
          (callee, res.toSeq)
        }
      })
    res
  }

  override def pretty(result: RDD[(VertexId, Seq[(Long, Int)])]): String = result.collect().toMap.toString

  override def explanation(): String = "a map of in-degrees of objects, per time"
}

case class MaxInDegree(p: Int => Boolean, name: String, spec: InDegreeSpec = HEAP_OR_STACK) extends VertexIdAnalyser {

  override def analyse(implicit g: SpencerDB) : DataFrame = {
    import g.sqlContext.implicits._
    //FIXME get rid of RDDs here
    val refs = (spec match {
      case HEAP          => g.selectFrame("refs", "SELECT callee, refstart, refend FROM refs WHERE kind = 'field'")
      case STACK         => g.selectFrame("refs", "SELECT callee, refstart, refend FROM refs WHERE kind = 'var'")
      case HEAP_OR_STACK => g.selectFrame("refs", "SELECT callee, refstart, refend FROM refs")
    })//.as[(Long, Option[Long], Option[Long])].rdd
      //.map(row => (row._1.asInstanceOf[VertexId], row._2, row._3))
      .rdd
      .groupBy(_.getAs[Long]("callee"))

    val testedObjs = refs.map(_._1)

    val passedObjs = refs
      .map({ case (callee, edgesIter) =>
        (callee, edgesIter.map(x =>
          (Option(x.getAs[Long]("refstart")), Option(x.getAs[Long]("refend")))
        ).toSeq.sortBy(_._1)) })
      .flatMap({ case (callee, edges) =>
        if (p(MaxInDegree.highestOverlap(edges))) {
          Some(callee)
        } else {
          None
        }
      }).filter(_ > 4)
    if (p(0)) {
      //the objects that were not in refs are those that have no references
      // pointing to them at all.. if p(0), we need to include them
      (passedObjs ++ (Obj().analyse.select("id").as[Long].rdd.subtract(testedObjs)))
        .toDF.withColumnRenamed("value", "id")
    } else {
      passedObjs.toDF.withColumnRenamed("value", "id")
    }
  }

  override def pretty(result: DataFrame): String = {
    "Objects where indegrees matched predicate ("+p+"):\n\t"+
      result.collect().mkString(", ")
  }

  override def toString: String = this.name

  override def explanation(): String = "the maximum in degree per object"

  override def getSQL = None //FIXME
}


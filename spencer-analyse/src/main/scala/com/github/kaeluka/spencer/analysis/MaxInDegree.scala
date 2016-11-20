package com.github.kaeluka.spencer.analysis
import com.datastax.spark.connector.CassandraRow
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import org.apache.spark.graphx.{VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

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

  override def analyse(implicit g: SpencerData): RDD[(VertexId, Seq[(Long, Array[VertexId])])] = {
    val refs = spec match {
      case HEAP => g.db.getTable("refs").filter(_.getString("kind") == "field")
      case STACK => g.db.getTable("refs").filter(_.getString("kind") == "var")
      case HEAP_OR_STACK => g.db.getTable("refs")
    }

    val res = refs
      .map(row => InRef(callee = row.getLong("callee"), start = row.getLong("start"), end = row.getLongOption("end"), holder = row.getLong("caller").asInstanceOf[VertexId]))
      .filter(_.callee == 28740)
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
}

case class InDegreeMap(spec: InDegreeSpec = HEAP_OR_STACK) extends SpencerAnalyser[RDD[(VertexId, Seq[(Long, Int)])]] {
  override def analyse(implicit g: SpencerData): RDD[(VertexId, Seq[(VertexId, Int)])] = {
    val refs = spec match {
      case HEAP => g.db.getTable("refs").filter(_.getString("kind") == "field")
      case STACK => g.db.getTable("refs").filter(_.getString("kind") == "var")
      case HEAP_OR_STACK => g.db.getTable("refs")
    }

    val res = refs
      .map(row => (row.getLong("callee").asInstanceOf[VertexId], row.getLong("start"), row.getLongOption("end")))
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

  override def pretty(result: RDD[(VertexId, Seq[(VertexId, Int)])]): String = result.collect().toMap.toString
}

case class MaxInDegree(p: Int => Boolean, spec: InDegreeSpec = HEAP_OR_STACK) extends SpencerAnalyser[RDD[Long]] {

  override def analyse(implicit g: SpencerData) : RDD[Long] = {
    val subgraph =
      spec match {
        case HEAP => g.graph.subgraph(epred = _.attr.kind == EdgeKind.FIELDSTORE)
        case STACK => g.graph.subgraph(epred = _.attr.kind == EdgeKind.VARSTORE)
        case HEAP_OR_STACK => g.graph
      }

    val refs: CassandraTableScanRDD[CassandraRow] = g.db.getTable("refs")

    (spec match {
      case HEAP => refs.filter(_.getString("kind") == "field")
      case STACK => refs.filter(_.getString("kind") == "var")
      case HEAP_OR_STACK => refs
    }).map(row => (row.getLong("callee").asInstanceOf[VertexId], row.getLongOption("start"), row.getLongOption("end")))
      .groupBy(_._1)
      .map({ case (callee, edgesIter) => (callee, edgesIter.map(x => (x._2, x._3)).toSeq.sortBy(_._1)) })
      .flatMap({ case (callee, edges) => {
        if (p(MaxInDegree.highestOverlap(edges))) {
          Some(callee)
        } else {
          None
        }
      }
      })
  }

  override def pretty(result: RDD[VertexId]): String = {
    "Objects where indegrees matched predicate ("+p+"):\n\t"+
      result.collect().mkString(", ")
  }

  override def toString: String = {
    if (p.toString().startsWith("<"))
      "InDegree(..., "+spec.toString+")"
    else {
      p.toString()+"()"
//      (p.toString()+" from "+spec.toString).replace(" ", "_")
    }
  }
}

//  override def toString(): String = {
//  }
//}

object RunUnique extends MaxInDegree(_ == 1) {}

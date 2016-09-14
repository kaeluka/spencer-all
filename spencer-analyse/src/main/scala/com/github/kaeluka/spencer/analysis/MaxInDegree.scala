package com.github.kaeluka.spencer.analysis
import com.datastax.spark.connector.CassandraRow
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import org.apache.spark.graphx.{VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

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
      .map({case (callee, edgesIter) => (callee, edgesIter.map(x=>(x._2, x._3)).toSeq.sortBy(_._1))})
      .flatMap({case (callee, edges) => {
        if (p(MaxInDegree.highestOverlap(edges))) {
          Some(callee)
        } else {
          None
        }
      }})
  }

  override def pretty(result: RDD[VertexId]): String = {
    "Objects where indegrees matched predicate ("+p+"):\n\t"+
      result.collect().mkString(", ")
  }

  override def toString: String = {
    if (p.toString().startsWith("<"))
      "InDegree(..., "+spec.toString+")"
    else {
      (p.toString()+" from "+spec.toString).replace(" ", "_")
    }
  }
}

//  override def toString(): String = {
//  }
//}

object RunUnique extends MaxInDegree(_ == 1) {}

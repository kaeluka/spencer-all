package com.github.kaeluka.spencer.analysis
import com.datastax.spark.connector.CassandraRow
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import com.github.kaeluka.spencer.tracefiles.CassandraSpencerDB
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._


object SpencerGraphImplicits {
  implicit def spencerDbToSpencerGraph(db: CassandraSpencerDB) : SpencerData = {
    new SpencerData(db)
  }

  implicit def spencerDbToFilteredGraph(db: CassandraSpencerDB): FilteredGraph = {
    new FilteredGraph(spencerDbToSpencerGraph(db).graph)
  }

  implicit def spencerDbToGraph(db: CassandraSpencerDB): Graph[ObjDesc, EdgeDesc] = {
    spencerDbToSpencerGraph(db).graph
  }

//  implicit def spencerDbToFilteredGraph(db: SpencerDB) : FilteredGraph = {
//    spencerGraphToFilteredGraph(spencerDbToSpencerGraph(db))
//  }

  implicit def filteredGraphToGraph(fg: FilteredGraph): Graph[ObjDesc, EdgeDesc] = {
    fg.graph
  }
}

@deprecated class SpencerData(val db: CassandraSpencerDB) {

  private def initGraph: Graph[ObjDesc, EdgeDesc] = {
    val objs = this.db.getTable("objects")
      .map(row => (row.getLong("id"), ObjDesc(klass = row.getStringOption("klass"))))
      .setName("object graph vertices")

//    val uses = this.db.getTable("uses")
//      .map(row => {
//        val fr = row.getLong("idx")
//        val to = fr + 1
//        Edge(
//          row.getLong("caller"),
//          row.getLong("callee"),
//          EdgeDesc(Some(fr), Some(to), EdgeKind.fromUsesKind(row.getString("kind")))
//        )
//      })
//      .setName("object graph edges")
    val refs = this.db.getTable("refs")
      .map(row => {
        val fr = row.getLongOption("refstart")
        val to = row.getLongOption("refend")
        Edge(
          row.getLong("caller"),
          row.getLong("callee"),
          EdgeDesc(fr, to, EdgeKind.fromRefsKind(row.getString("kind"))))
      })

    val g: Graph[ObjDesc, EdgeDesc] =
      Graph(objs, refs)
    g.cache()
    g
  }

  private val g = initGraph

  def graph: Graph[ObjDesc, EdgeDesc] = g
}

case class ObjDesc(klass: Option[String])

case class EdgeDesc(fr: Option[Long], to: Option[Long], kind: EdgeKind.EdgeKind)

object EdgeKind extends Enumeration {
  type EdgeKind = Value
  val FIELDSTORE, FIELDLOAD, VARSTORE, VARLOAD, READ, MODIFY, FIELD, VAR = Value
  def fromRefsKind(s: String) : Value = {
    s match {
//      case "fieldstore" => FIELDSTORE
//      case "fieldload"  => FIELDLOAD
//      case "varstore"   => VARSTORE
//      case "varload"    => VARLOAD
//      case "read"       => READ
//      case "modify"     => MODIFY
      case "field"      => FIELD
      case "var"        => VAR
    }
  }

  def fromUsesKind(s: String) : Value = {
    s match {
      case "fieldstore" => FIELDSTORE
      case "fieldload"  => FIELDLOAD
      case "varstore"   => VARSTORE
      case "varload"    => VARLOAD
      case "read"       => READ
      case "modify"     => MODIFY
      //      case "field"      => FIELD
      //      case "var"        => VAR
    }
  }
}

object UseKind extends Enumeration {
  type UseKind = Value
  val READ, MODIFY = Value
  def fromEventKind(s: String) : Value = {
    s match {
      case "fieldstore" => MODIFY
      case "fieldload"  => READ
      case "varstore"   => READ
      case "varload"    => READ
      case "read"       => READ
      case "modify"     => MODIFY
      case "field"      => ???
      case "var"        => ???
    }
  }
}

/**
  * A graph that can be filtered, using the builder pattern.
  * @param graph the graph to be filtered
  */
class FilteredGraph(val graph: Graph[ObjDesc, EdgeDesc]) {
  def onlyEdgesToInstances(klass: String): FilteredGraph = {
    new FilteredGraph(
      this.graph.subgraph(
        _.dstAttr.klass.contains(klass),
        (_, _) => true))
  }

  def onlyEdgesToInstances(klass: Class[_]): FilteredGraph = {
    new FilteredGraph(
      this.graph.subgraph(
        _.dstAttr.klass.contains(klass.getName),
        (_, _) => true))
  }
}

package com.github.kaeluka.spencer.analysis
import com.github.kaeluka.spencer.PostgresSpencerDB
import org.apache.spark.graphx._


object SpencerGraphImplicits {

//  implicit def spencerDbToFilteredGraph(db: PostgresSpencerDB): FilteredGraph = {
//    new FilteredGraph(spencerDbToSpencerGraph(db).graph)
//  }

//  implicit def spencerDbToGraph(db: PostgresSpencerDB): Graph[ObjDesc, EdgeDesc] = {
//    spencerDbToSpencerGraph(db).graph
//  }

//  implicit def spencerDbToFilteredGraph(db: SpencerDB) : FilteredGraph = {
//    spencerGraphToFilteredGraph(spencerDbToSpencerGraph(db))
//  }

  implicit def filteredGraphToGraph(fg: FilteredGraph): Graph[ObjDesc, EdgeDesc] = {
    fg.graph
  }
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

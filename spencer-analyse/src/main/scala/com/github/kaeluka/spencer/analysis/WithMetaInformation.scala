package com.github.kaeluka.spencer.analysis

import java.sql.ResultSet

import com.github.kaeluka.spencer.PostgresSpencerDB
import com.google.common.base.Stopwatch
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

case class ObjWithMeta(oid: VertexId,
                       klass: Option[String],
                       allocationSite: Option[String],
                       firstusage: Long,
                       lastusage: Long,
                       thread: Option[String],
                       numFieldWrites: Long,
                       numFieldReads: Long,
                       numCalls: Long)

case class ConnectedComponent() extends VertexIdAnalyser {
  override def analyse(implicit g: PostgresSpencerDB): DataFrame = {
    import g.sqlContext.implicits._
    val graph: Graph[ObjDesc, EdgeDesc] = g.getGraph()
    val components: Graph[VertexId, EdgeDesc] = graph.subgraph(epred = _.attr.kind == EdgeKind.FIELD).connectedComponents()

    val joined = graph.outerJoinVertices(components.vertices) {
      (vid, _odesc, optCC) => optCC
    }

    val data = joined.vertices.toDF.withColumnRenamed("_1", "id").withColumnRenamed("_2", "connectedComponent")

    data.show(10)

    data
  }

  override def explanation(): String = "are connected"

  override def getSQLBlueprint = ??? ///FIXME
  override def getVersion = { 0 }
}

case class WithMetaInformation(inner: VertexIdAnalyser) extends VertexIdAnalyser {

  override def analyse(implicit g: PostgresSpencerDB): DataFrame = {
    println("getting meta info")
    println("WARNING: GETTING ALL META INFO! USE JOINS!")
    super.analyse
  }

  override def pretty(result: DataFrame): String = {
    result.toString()
  }

  def availableVariables : Map[String,String] = {
    Map(
      "klass"              -> "categorical",
      "allocationSite"     -> "categorical",
      "firstusage"         -> "numerical",
      "lastusage"          -> "numerical",
      "thread"             -> "categorical",
//      "numFieldWrites"     -> "numerical",
//      "numFieldReads"      -> "numerical",
      "numCalls"           -> "numerical"
    )
  }

  override def explanation(): String = inner.explanation()

  override def getVersion = { 0 }

  override def getSQLBlueprint = {
    """SELECT
      |  id,
      |  klass AS klass,
      |  allocationsitefile AS allocationsitefile,
      |  allocationsiteline AS allocationsiteline,
      |  firstusage AS firstusage,
      |  lastusage AS lastusage,
      |  COUNT(calls.callee) as numCalls,
      |  objects.thread as thread
      |FROM objects
      |LEFT OUTER JOIN calls ON calls.callee = objects.id
      |GROUP by objects.id
      |""".stripMargin
  }
}

object WithMetaInformationTest extends App {

  implicit val db: PostgresSpencerDB = new PostgresSpencerDB("test")
  db.connect()

  val watch: Stopwatch = Stopwatch.createStarted()
  AgeOrderedObj().analyse.show()
  println("analysis took "+watch.stop())
}
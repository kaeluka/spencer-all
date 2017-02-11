package com.github.kaeluka.spencer.analysis

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
  def analyse(implicit g: SpencerDB): DataFrame = {
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

  override def getSQL = None //FIXME
}

case class WithMetaInformation(inner: VertexIdAnalyser) extends SpencerAnalyser[DataFrame] {

  override def analyse(implicit g: SpencerDB): DataFrame = {
    import g.sqlContext.implicits._
    val matchingIDs = inner.analyse(g)

    println("getting meta info")
    println("WARNING: GETTING ALL META INFO! USE JOINS!")

    g.getFrame("calls").createOrReplaceTempView("calls")
    g.getFrame("uses").createOrReplaceTempView("uses")
    g.selectFrame("objects",
        """SELECT
      |  id,
      |  first(klass) AS klass,
      |  first(allocationsitefile) AS allocationsitefile,
      |  first(allocationsiteline) AS allocationsiteline,
      |  first(firstusage) AS firstusage,
      |  first(lastusage) AS lastusage,
      |  COUNT(calls.callee) as numCalls,
      |  first(objects.thread) as thread
      |FROM objects
      |LEFT OUTER JOIN calls ON calls.callee = objects.id
      |GROUP by objects.id
      |""".stripMargin)
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

}

object WithMetaInformationTest extends App {

  implicit val db: SpencerDB = new PostgresSpencerDB("test")
  db.connect()

  val watch: Stopwatch = Stopwatch.createStarted()
  AgeOrderedObj().analyse.show()
  println("analysis took "+watch.stop())
}
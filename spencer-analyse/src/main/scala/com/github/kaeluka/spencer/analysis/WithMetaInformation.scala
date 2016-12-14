package com.github.kaeluka.spencer.analysis

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

case class ObjWithMeta(oid: VertexId,
                       klass: Option[String],
                       allocationSite: Option[String],
                       firstUsage: Long,
                       lastUsage: Long,
                       thread: Option[String])

case class WithMetaInformation(inner: VertexIdAnalyser) extends SpencerAnalyser[RDD[ObjWithMeta]] {

  override def analyse(implicit g: SpencerDB): RDD[ObjWithMeta] = {
    import g.sqlContext.implicits._
    val matchingIDs = inner.analyse(g).toDF

    println("getting meta info")
    println("WARNING: GETTING ALL META INFO! USE JOINS!")

    val frame = g.selectFrame("objects", "SELECT id, klass, allocationsitefile, allocationsiteline, firstusage, lastusage, thread " +
      "FROM objects")
//      .where($"id" isin matchingIDs)
    frame.show(10)
    val ret =frame
      .rdd
      .map(row =>
        ObjWithMeta(
          oid = row.getAs[Long]("id"),
          klass = Option(row.getAs[String]("klass")),
          allocationSite = Option(row.getAs[String]("allocationsitefile"))
            .flatMap(file =>
              Option(row.getAs[Long]("allocationsiteline"))
                .map(line => file+":"+line.toString))
            .filter(! _.contains("<")),
          firstUsage = row.getAs[Long]("firstusage"),
          lastUsage = row.getAs[Long]("lastusage"),
          thread = Option(row.getAs[String]("thread"))
        ))
    println("gotten meta info!")
    ret
  }

  override def pretty(result: RDD[ObjWithMeta]): String = {
    "Satisfying "+inner+":\n\t"+
      result.collect().mkString("\n")
  }

  def availableVariables : Map[String,String] = {
    Map(
    "klass"          -> "categorical",
    "allocationSite" -> "categorical",
    "firstUsage"     -> "ordinal",
    "lastUsage"      -> "ordinal",
    "thread"         -> "categorical"
    )
  }

  override def explanation(): String = inner.explanation()

}


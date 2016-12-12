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

  override def analyse(implicit g: SpencerData): RDD[ObjWithMeta] = {
    val matchingIDs: RDD[VertexId] = inner.analyse(g)

    //    matchingIDs.
    g.db.getTable("objects")
      .select("id", "klass", "allocationsitefile", "allocationsiteline", "firstusage", "lastusage", "thread")
      .where("id IN ?", matchingIDs.collect().toList)
      .map(row =>
        ObjWithMeta(
          oid = row.getLong("id"),
          klass = row.getStringOption("klass"),
          allocationSite = row.getStringOption("allocationsitefile")
            .flatMap(file => row.getLongOption("allocationsiteline").map(file+":"+_))
            .filter(! _.contains("<")),
          firstUsage = row.getLong("firstusage"),
          lastUsage = row.getLong("lastusage"),
          thread = row.getStringOption("thread")
        ))
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


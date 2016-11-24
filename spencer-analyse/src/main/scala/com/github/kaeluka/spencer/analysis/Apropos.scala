package com.github.kaeluka.spencer.analysis
import com.github.kaeluka.spencer.tracefiles._
import org.apache.spark.graphx.VertexId


case class Apropos(id: VertexId) extends SpencerAnalyser[AproposData] {
  override def analyse(implicit g: SpencerData): AproposData = {
    g.db.aproposObject(id)
  }

  override def pretty(result: AproposData): String = {
    result.toString
  }

  override def explanation(): String = "the history of an object"
}

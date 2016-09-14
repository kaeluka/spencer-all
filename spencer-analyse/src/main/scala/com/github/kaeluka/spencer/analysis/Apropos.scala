package com.github.kaeluka.spencer.analysis
import org.apache.spark.graphx.VertexId

case class Apropos(id: VertexId) extends SpencerAnalyser[String] {
  override def analyse(implicit g: SpencerData): String = {
    g.db.aproposObject(id)
  }

  override def pretty(result: String): String = {
    result
  }
}

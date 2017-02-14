package com.github.kaeluka.spencer.analysis

import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD

trait Observation extends SpencerAnalyser[RDD[(VertexId, Double)]] {
  override def pretty(result: RDD[(VertexId, Double)]): String = {
      this.explanation()+"\n"+
        result
          .take(100)
          .mkString("\n")+
        (if (result.count() > 100) {
          "..."
        } else {
          ""
    })
  }
}

//case class LifeTimeObs() extends Observation {
//  override def analyse(implicit g: SpencerDB): RDD[(VertexId, Double)] = {
//    null
//  }
//
//  override def explanation(): String = "life time of an object"
//}

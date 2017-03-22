package com.github.kaeluka.spencer.analysis
import com.github.kaeluka.spencer.PostgresSpencerDB

case class Apropos(id: Long) extends SpencerAnalyser[AproposData] {
  override def analyse(implicit g: PostgresSpencerDB): AproposData = {
    g.aproposObject(id)
  }

  override def explanation(): String = "the history of an object"
}

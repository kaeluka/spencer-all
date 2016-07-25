package com.github.kaeluka.spencer.analysis
import com.github.kaeluka.spencer.tracefiles.SpencerDB

object AproposAnalyser extends SpencerDBAnalyser {
  override def setUp(db: SpencerDB): Unit = {}

  override def analyse(db: SpencerDB): Unit = {
    println(db.aproposObject(93198))
  }

  override def tearDown(db: SpencerDB): Unit = {}
}

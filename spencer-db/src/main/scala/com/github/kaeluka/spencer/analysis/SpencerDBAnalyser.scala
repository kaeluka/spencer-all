package com.github.kaeluka.spencer.analysis

import com.datastax.driver.core.Session
import com.github.kaeluka.spencer.tracefiles.SpencerDB

trait SpencerDBAnalyser extends App {
  def setUp(db: SpencerDB);
  def analyse(db: SpencerDB);
  def tearDown(db: SpencerDB);

  override def main(args: Array[String]) {
    val db: SpencerDB = new SpencerDB("test")
    db.connect()

    println("running analyser "+this.getClass.getName)
    try {
      this.analyse(db)
      try {
        this.setUp(db)
      } catch {
        case e => {
          println("setting up failed:")
          e.printStackTrace()
        }
      }
    } catch {
      case e => {
        println("Anaylsis failed:")
        e.printStackTrace()
      }
    }
    try {
      this.tearDown(db)
    } catch {
      case e => {
        println("tearing down failed:")
        e.printStackTrace()
      }
    }

    System.exit(0)
  }
}

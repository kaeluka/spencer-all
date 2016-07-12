package com.github.kaeluka.spencer.analysis
import com.datastax.driver.core.Session
import com.github.kaeluka.spencer.DBLoader
import com.github.kaeluka.spencer.tracefiles.SpencerDB
import org.apache.spark.SparkContext
import com.datastax.spark.connector._

/**
  * Created by stebr742 on 2016-07-06.
  */
object CountEvents extends SpencerDBAnalyser {
  override def setUp(db: SpencerDB): Unit = {}

  override def analyse(db: SpencerDB): Unit = {
  }

  override def tearDown(db: SpencerDB): Unit = {}

}

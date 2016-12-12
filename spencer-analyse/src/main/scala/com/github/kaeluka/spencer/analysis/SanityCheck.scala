package com.github.kaeluka.spencer.analysis
import com.github.kaeluka.spencer.SpencerLoad
import com.github.kaeluka.spencer.tracefiles.SpencerDB
import org.apache.spark.SparkContext
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import com.datastax.spark.connector._
import com.google.common.base.Stopwatch
import org.apache.spark.storage.StorageLevel


object SanityCheck extends SpencerAnalyser[Unit] {

  override def analyse(implicit g: SpencerData): Unit = {
    val watch: Stopwatch = Stopwatch.createStarted()
    val callers = SpencerDB.sc.cassandraTable(g.db.session.getLoggedKeyspace, "refs")
      .select("caller")
      .map(_.getLong("caller"))
      .filter(_ > 0)
      .distinct()
      .setName("get all callers")
    val callees = SpencerDB.sc.cassandraTable(g.db.session.getLoggedKeyspace, "refs")
      .select("callee")
      .map(_.getLong("callee"))
      .filter(_ > 0)
      .distinct()
      .setName("get all callees")
    val objects: RDD[Long] = SpencerDB.sc.cassandraTable(g.db.session.getLoggedKeyspace, "objects")
      .select("id")
      .distinct()
      .map(_.getLong("id"))
      .setName("get all object IDs")
//    println("have got "+caller.count+" callers")
//    println("have got "+callees.count+" callees")
//    println("have got "+objects.count+" objects")

//    println("objects ====================")
//    objects.foreach(println(_))

    println("callees ====================")
//    callees.foreach(println(_))
    //    val notObjects = callees.filter( !objects.contains(_) ).collect()
    val uninitCallees: RDD[Long] = callees.subtract(objects)
    println(uninitCallees
      .takeSample(withReplacement = false, 30, seed = 0)
      .map(handleUninitCallee(g.db, _))
      .mkString("\n"))
    println("...\nTOTAL: " + uninitCallees.count + " objects")

    val uninitCallers: RDD[Long] = callers.subtract(objects)
    println(uninitCallers.takeSample(withReplacement = false, 30).map("caller but never initialised: " + _).mkString("\n"))
    println("...\nTOTAL: " + uninitCallers.count + " objects")
    println("sanity check took "+watch.stop)
    //    objects.zip(callees).foreach(ab =>
//      ab match {
//        case (a, b) => {
//          println("a="+a+", b="+b)
//          if (a != b) {
//            throw new IllegalArgumentException("sets not equal (" + a + ", " + b + ")")
//          }
//        }
//      })
        //    caller.foreach(row => {
        //
        //      val caller: Long = row.getLong("caller")
        //      println(caller)
        //      if (! objects.)
        //    }
        //    )
//    sys.exit(0)
  }

  def handleUninitCallee(db: SpencerDB, callee: Long) : String = {
    "callee but never initialised: " + callee + "\n----------\n" +
      db.aproposObject(callee) + "\n==========\n"
  }

  override def pretty(result: Unit): String = ""

  override def explanation(): String = "a sanity check"

}

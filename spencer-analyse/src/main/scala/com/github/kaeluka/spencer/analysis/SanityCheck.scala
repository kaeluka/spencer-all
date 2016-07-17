package com.github.kaeluka.spencer.analysis
import com.github.kaeluka.spencer.DBLoader
import com.github.kaeluka.spencer.tracefiles.SpencerDB
import org.apache.spark.SparkContext
import com.datastax.spark.connector._

object SanityCheck extends SpencerDBAnalyser {
  override def setUp(db: SpencerDB): Unit = { }

  override def analyse(db: SpencerDB): Unit = {
    val sc: SparkContext = DBLoader.startSpark()
    val caller = sc.cassandraTable(db.session.getLoggedKeyspace, "refs").select("caller").map(_.getLong("caller")).distinct().sortBy(x => x).filter(_ > 0)
    val callees = sc.cassandraTable(db.session.getLoggedKeyspace, "refs").select("callee").map(_.getLong("callee")).distinct().sortBy(x => x).filter(_ > 0)
    val rowSet: Set[CassandraRow] = sc.cassandraTable(db.session.getLoggedKeyspace, "objects").select("id").distinct().collect().toSet
    val objects = rowSet.map(_.getLong("id"))
    println("have got "+caller.count()+" callers")
    println("have got "+callees.count()+" callees")
    println("have got "+objects.size+" objects")

//    println("objects ====================")
//    objects.foreach(println(_))


    println("callees ====================")
//    callees.foreach(println(_))
    val notObjects = callees.filter( !objects.contains(_) ).collect()
    notObjects.foreach(tag => println("not recognised: "+tag))
    println(notObjects.length+" objects were not recognised")

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

      }

    override def tearDown(db: SpencerDB): Unit = { }
  }

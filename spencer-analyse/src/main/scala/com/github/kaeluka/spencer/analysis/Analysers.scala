package com.github.kaeluka.spencer.analysis

import java.io._
import java.nio.charset.StandardCharsets

import com.github.kaeluka.spencer.tracefiles.CassandraSpencerDB
import com.google.common.base.Stopwatch
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.objectweb.asm.ClassReader
import org.objectweb.asm.util.TraceClassVisitor

import scala.language.implicitConversions

trait SpencerAnalyser[T] {
  def analyse(implicit g: SpencerDB) : T
  def pretty(result: T): String
  def explanation(): String
}

object SpencerAnalyserUtil {
  def rddToString[T](rdd: RDD[T]) : String = {
    val count = rdd.count()
    if (count > 50) {
      rdd
        .takeSample(withReplacement = false, num = 50, seed = 0)
        .mkString(count+" x\t- [ ", ", ", ", ... ]")
    } else {
      rdd.collect().mkString(count+" x\t- [ ", ", ", " ]")
    }
  }
}

/*
case class Snapshotted[T: ClassTag](inner : SpencerAnalyser[RDD[T]]) extends SpencerAnalyser[RDD[T]] {
  override def analyse(implicit g: SpencerDB): RDD[T] = {
    if (inner.isInstanceOf[Snapshotted[_]]) {
      inner.analyse
    } else {
      val watch: Stopwatch = Stopwatch.createStarted()
      val queryString = inner.toString
      val snapshots = g.db.getTable("snapshots").where("query = ?", queryString)
      assert(snapshots.count == 0 || snapshots.count == 1)

      if (snapshots.count == 0) {
        val result = inner.analyse.cache()
        val baostream: ByteArrayOutputStream = new ByteArrayOutputStream()
        val ostream: ObjectOutputStream = new ObjectOutputStream(baostream)
        ostream.writeObject(result.collect())

        val resultSerialised : ByteBuffer = ByteBuffer.wrap(baostream.toByteArray)
        println(s"snapshot for query $queryString: writing ${resultSerialised.array().length} bytes")
        g.db.session.execute("INSERT INTO "+g.db.session.getLoggedKeyspace+".snapshots (query, result) VALUES (?, ?);", queryString, resultSerialised)
        result
      } else {
        val resultSerialised = snapshots.first().getBytes("result")
        println(s"snapshot for query $queryString: reading ${resultSerialised.array().length} bytes")
        val ois: ObjectInputStream = new ObjectInputStream(new ByteArrayInputStream(resultSerialised.array()))
        val res = CassandraSpencerDB.sc.parallelize(ois.readObject().asInstanceOf[Array[T]])
        println(s"deserialising took $watch")
        res
      }
    }
  }

  override def pretty(result: RDD[T]): String = inner.pretty(result)

  override def toString: String = inner.toString

  override def explanation(): String = inner.explanation()
}
*/

case class SourceCode(klass: String) extends SpencerAnalyser[Option[String]] {
  override def analyse(implicit g: SpencerDB): Option[String] = {
    import g.sqlContext.implicits._
    val result =
      g.selectFrame("classdumps", s"SELECT bytecode FROM classdumps WHERE classname = $klass").as[Array[Byte]].rdd
    assert(result.count() <= 1)
    if (result.count() == 0) {
      None
    } else {
      val bytecode = result.first()

      val classreader = new ClassReader(bytecode)
      val baos = new ByteArrayOutputStream()
      val sw : PrintWriter = new PrintWriter(new PrintStream(baos))
      classreader.accept(new TraceClassVisitor(sw), ClassReader.EXPAND_FRAMES)
      Some(new String(baos.toByteArray, StandardCharsets.UTF_8))
    }
  }

  override def pretty(result: Option[String]): String = result.toString

  override def explanation(): String = "shows the source code of a class"
}

case class Timed[T](inner: SpencerAnalyser[T]) extends SpencerAnalyser[T] {
  private val duration : Stopwatch = Stopwatch.createUnstarted()

  override def analyse(implicit g: SpencerDB): T = {
    duration.start()
    val ret = inner.analyse(g)
    duration.stop()
    ret
  }

  override def pretty(result: T): String = {
    duration.start()
    val iPretty = inner.pretty(result)
    duration.stop()
    this.toString+":\n"+iPretty
  }

  override def toString: String = {
      inner.toString + " (took "+duration.toString+")"
  }

  override def explanation(): String = inner.explanation()
}

case class LifeTime(inner: VertexIdAnalyser) extends SpencerAnalyser[RDD[(VertexId, (Long, Long))]] {

  override def analyse(implicit g: SpencerDB): RDD[(VertexId, (Long, Long))] = {
    import g.sqlContext.implicits._
    //FIXME: uses collect
    val innerRes = inner.analyse.toDF("id")
    g.selectFrame("objects", "SELECT id, firstusage, lastusage FROM objects")
      .where($"id" isin innerRes).as[(Long, (Long, Long))].rdd
//    g.db.getTable("objects")
//        .select("id", "firstusage", "lastusage")
//        .where("id IN ?", inner.analyse.collect().toList)
//        .map(row => (row.getLong("id"), (row.getLong("firstusage"), row.getLong("lastusage"))))

  }

  override def pretty(result: RDD[(VertexId, (Long, Long))]): String = {
    "Lifetimes:\n\t"+result.collect().mkString(", ")
  }

  override def explanation(): String = "shows the first and last times objects were used"
}

/*
case class ObjsByClass() extends SpencerAnalyser[RDD[(String, Iterable[VertexId])]] {
  override def analyse(implicit g: SpencerDB): RDD[(String, Iterable[VertexId])] = {
    g.db.getTable("objects")
      .groupBy(_.getStringOption("klass"))
      .filter(_._1.isDefined)
//        .filter({case ()})
      .map({ case (optKlass, objects) => (optKlass.get, objects.map(_.getLong("id")))})
  }

  override def pretty(result: RDD[(String, Iterable[VertexId])]): String = {
    result.sortBy(_._2.size).collect().map({case (klass, objs) => klass + " - ("+ objs.size + " instances): \n\t\t" + objs.mkString("[", ", ", "]")}).mkString("\n")
  }

  override def explanation(): String = "grouped by allocation site"
}
*/

case class Collect[T](inner : SpencerAnalyser[RDD[T]]) extends SpencerAnalyser[Array[T]] {
  override def analyse(implicit g: SpencerDB): Array[T] = {
    inner.analyse.collect()
  }

  override def pretty(result: Array[T]): String = {
    this.toString+":\n\t"+result.mkString("[ ", ", ", " ]")
  }

  override def explanation(): String = inner.explanation()
}

/*
case class GroupByClass(inner: VertexIdAnalyser) extends SpencerAnalyser[RDD[(Option[String], Iterable[VertexId])]] {
  override def analyse(implicit g: SpencerDB): RDD[(Option[String], Iterable[VertexId])] = {
    val innerCollected = inner.analyse.collect()
    g.db.getTable("objects")
      .select("klass", "id")
      .where("id IN ?", innerCollected.toList)
      .map(row => (row.getStringOption("klass"), row.getLong("id")))
      .groupBy(_._1)
      .map {
        case (oKlass, iter) =>
          (oKlass, iter.map(_._2))
      }
  }

  override def pretty(result: RDD[(Option[String], Iterable[VertexId])]): String = {
    val collected =
      (if (result.count() < 1000) {
        result.sortBy(_._2.size*(-1))
      } else {
        result
      }).collect()

    this.toString+":\n"+
      collected.map({
        case (allocationSite, instances) =>
          val size = instances.size
          allocationSite+"\t-\t"+(if (size > 50) {
            instances.take(50).mkString("\t"+size+" x - [ ", ", ", " ... ]")
          } else {
            instances.mkString("\t"+size+" x - [ ", ", ", " ]")
          })
      }).mkString("\n")
  }

  override def explanation(): String = "some objects, grouped by class"
}
*/

/*
case class PerClass[U: ClassTag](f : (Option[String], Iterable[VertexId]) => Option[U]) extends SpencerAnalyser[RDD[U]] {
  override def analyse(implicit g: SpencerDB): RDD[U] = {
    val innerObjs: Set[VertexId] = Obj().analyse.collect().toSet[VertexId]
    val grouped: RDD[(Option[String], Iterable[VertexId])] =
      GroupByClass(Obj()).analyse
    if (classTag[U].toString.contains("RDD") /* hack */) {
      grouped.flatMap({case (loc, it) => f(loc, it)})
    } else {
      CassandraSpencerDB.sc.parallelize(grouped.collect().flatMap({case (loc, it) => f(loc, it)}))
    }
  }

  override def pretty(result: RDD[U]): String = {
    result.collect().mkString(this.toString+":\n\t - ", "\n\t - ", "")
  }

  override def explanation(): String = "grouped by class, then mapped"
}
*/

/*
case class PerAllocationSite[U: ClassTag](f : ((Option[String], Option[Long]), Iterable[VertexId]) => Option[U]) extends SpencerAnalyser[RDD[U]] {
  override def analyse(implicit g: SpencerDB): RDD[U] = {
    val innerObjs: Set[VertexId] = Obj().analyse.collect().toSet[VertexId]
    val grouped: RDD[((Option[String], Option[Long]), Iterable[VertexId])] =
      GroupByAllocationSite(Obj()).analyse
    if (classTag[U].toString.contains("RDD") /* hack */) {
      grouped.flatMap({case (loc, it) => f(loc, it)})
    } else {
      CassandraSpencerDB.sc.parallelize(grouped.collect().flatMap({case (loc, it) => f(loc, it)}))
    }
  }

  override def pretty(result: RDD[U]): String = {
    result.collect().mkString(this.toString+":\n\t - ", "\n\t - ", "")
  }

  override def explanation(): String = "grouped by allocation sites and then mapped"
}
*/

/*
object ProportionPerClass {
  def apply(inner: VertexIdAnalyser)(implicit g: SpencerDB) : SpencerAnalyser[RDD[(Option[String], (Int, Int))]] = {
    val innerSet = inner.analyse.collect().toSet
    PerClass({case (loc, iter) => {
      val instances: Set[VertexId] = iter.toSet
      Some((loc, ((instances intersect innerSet).size, instances.size)))
    }})
  }
}
*/

/*
object ProportionPerAllocationSite {
  def apply(inner: VertexIdAnalyser)(implicit g: SpencerDB) : SpencerAnalyser[RDD[((Option[String], Option[Long]), (Int, Int))]] = {
    val innerSet = inner.analyse.collect().toSet
    PerAllocationSite({case (loc, iter) => {
      val allocSiteObjs: Set[VertexId] = iter.toSet
      Some((loc, ((allocSiteObjs intersect innerSet).size, allocSiteObjs.size)))
    }})
  }
}
*/

case class ConnectedComponent(marker: VertexId, members: Array[VertexId]) {
  override def toString(): String = {
    "ConnectedComponent("+marker+", "+members.mkString("{", ",", "}")+")"
  }
}

case class ConnectedComponents(minSize: Int) extends SpencerAnalyser[RDD[ConnectedComponent]] {
  def analyse(implicit g: SpencerDB): RDD[ConnectedComponent] = {
    val graph: Graph[ObjDesc, EdgeDesc] = g.getGraph()
    val components: Graph[VertexId, EdgeDesc] = graph.subgraph(epred = _.attr.kind == EdgeKind.FIELD).connectedComponents()

    val compSet = components.vertices
    compSet
      .groupBy(_._2) // group by group ID
      .map { case (marker, iter) => ConnectedComponent(marker, iter.map(_._1).toArray) }
      .filter(_.members.size >= minSize)
  }

  def pretty(result: RDD[ConnectedComponent]) : String = result.collect().mkString("\n")

  override def explanation(): String = "connected"
}

object Scratch extends App {

  run

  def run: Unit = {
    implicit val db: CassandraSpencerDB = new CassandraSpencerDB("test")
    db.connect()

    val watch: Stopwatch = Stopwatch.createStarted()

    val query =
//      InRefsHistory()
      ImmutableObj()
//        ProportionPerAllocationSite(Deeply(MutableObj()) and ObjWithInstanceCountAtLeast(10))
//        InstanceOfClass("Foo") vs MaxInDegree(MaxInDegree.Unique, InDegreeSpec.HEAP)
//        InstanceOfClass("Foo") vs (ImmutableObj() and ImmutableObj())
//        InstanceOfClass("java.util.TreeSet")
//        MutableObj()
//        DeeplyImmutableClass()
//        InDegree(InDegree.Aliased, InDegreeSpec.HEAP) vs InDegree(InDegree.Aliased, InDegreeSpec.STACK)
//        Mutable() vs InDegree(InDegree.Aliased)
//        (InstanceOfClass("Foo") and Mutable()) vs (InstanceOfClass("Foo") and InDegree(InDegree.Aliased))
//        Apropos(28740)
//        ConnectedComponents(10)
//        ProportionOutOf(InstanceOfClass("[C"), AllObjects())
//        WithClassName(InDegree(_ > 1) and Mutable())
//        ConnectedWith(InstanceOfClass("Foo")) and Mutable()
//        Apropos(91947)
//        WithClassName(ReachableFrom(InstanceOfClass("java.lang.String")))
//        Tabulate(AllClasses(), (klass: String) => ProportionOutOf(Immutable(), InstanceOfClass(klass)))

    val res = query.analyse
    println("name: "+query.toString)
    println("expl: "+query.explanation())

    //  res.collect().foreach(id => {
    //    val query = Apropos(id)
    //    val res = query.analyse
    //    println(query.pretty(res))
    //    println("===============")
    //  })
    println(query.pretty(res))

    //  val map = res.toMap.mapValues(x => x.collect())

    //  println(map.map({
    //    case (klass, objs) => objs.mkString("class " + klass + ":\t{ ", ", ", " }")
    //  }).mkString("\n"))

    println("analysis took " + watch.stop())
    db.shutdown()
  }
}


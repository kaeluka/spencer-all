package com.github.kaeluka.spencer.analysis

import java.io._
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import com.datastax.spark.connector.CassandraRow
import com.github.kaeluka.spencer.analysis.EdgeKind.EdgeKind
import com.github.kaeluka.spencer.tracefiles.SpencerDB
import com.google.common.base.Stopwatch
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.objectweb.asm.ClassReader
import org.objectweb.asm.util.TraceClassVisitor

import scala.language.implicitConversions
import scala.reflect.{ClassTag, _}

trait SpencerAnalyser[T] {
  def analyse(implicit g: SpencerData) : T
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
case class Klass() extends SpencerAnalyser[RDD[String]] {
  override def analyse(implicit g: SpencerData): RDD[String] = {
    g.db.getTable("objects")
      .select("klass")
      .map(_.getStringOption("klass")).filter(_.nonEmpty).map(_.get)
      .distinct()
  }

  override def pretty(result: RDD[String]): String = result.collect().mkString(this.toString+":\n\t { ", ", ", " }")
}
*/

case class Snapshotted[T: ClassTag](inner : SpencerAnalyser[RDD[T]]) extends SpencerAnalyser[RDD[T]] {
  override def analyse(implicit g: SpencerData): RDD[T] = {
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
        val res = g.db.sc.parallelize(ois.readObject().asInstanceOf[Array[T]])
        println(s"deserialising took $watch")
        res
      }
    }
  }

  override def pretty(result: RDD[T]): String = inner.pretty(result)

  override def toString: String = inner.toString

  override def explanation(): String = inner.explanation()
}

/*
case class RddAnalyser[T](inner: SpencerAnalyser[RDD[T]]) extends SpencerAnalyser[RDD[T]] {
  override def analyse(implicit g: SpencerData): RDD[T] = inner.analyse
  override def pretty(result: RDD[T]): String = inner.pretty(result)

  def groupBy[U:ClassTag](f: T => U): SpencerAnalyser[RDD[(U, Iterable[T])]] = {
    new SpencerAnalyser[RDD[(U, Iterable[T])]] {
      override def pretty(result: RDD[(U, Iterable[T])]): String = {
        result
          .map({case (u, t) => u+"\t->\t"+t})
          .collect()
          .mkString(
            "grouped:\n\t[ "
            , ", "
            , "] ")
      }

      override def analyse(implicit g: SpencerData): RDD[(U, Iterable[T])] = {
        inner.analyse.groupBy(f)
      }
    }
  }
}
*/

//object TableAnalyserImplicits {
//  implicit def toTableAnalyser[T,U](x: SpencerAnalyser[(T,U)]) : SpencerTableAnalyser = {
//    null
//  }
//
//abstract class SpencerTableAnalyser {
//  def within[T](population: SpencerAnalyser[RDD[T]]): SpencerAnalyser[Double] = {
//    null
//  }
//}

case class SourceCode(klass: String) extends SpencerAnalyser[Option[String]] {
  override def analyse(implicit g: SpencerData): Option[String] = {
    val result =
      g.db.getTable("classdumps").where("classname = ?", klass).select("bytecode").collect()
    assert(result.length <= 1)
    if (result.length == 0) {
      None
    } else {
      val bytecodeBuffer = result(0).getBytes("bytecode")
      val bytecode = new Array[Byte](bytecodeBuffer.remaining())
      bytecodeBuffer.get(bytecode)

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

/*
case class ProportionIn[T](smaller: SpencerAnalyser[RDD[T]], larger: SpencerAnalyser[RDD[T]]) extends SpencerAnalyser[(RDD[T], RDD[T])] {

  override def analyse(implicit g: SpencerData): (RDD[T], RDD[T]) = {
    (smaller.analyse(g), larger.analyse(g))
  }

  override def pretty(result: (RDD[T], RDD[T])): String = {
    result match {
      case (resSmall, resLarge) =>
        val smallCount: Long = resSmall.count()
        val largeCount: Long = resLarge.count()
        val prop = smallCount*100.0/largeCount
        prop+"%, or "+smallCount+"/"+largeCount+" of "+this.larger+" are "+smaller
    }
  }
}
*/

case class Timed[T](inner: SpencerAnalyser[T]) extends SpencerAnalyser[T] {
  private val duration : Stopwatch = Stopwatch.createUnstarted()

  override def analyse(implicit g: SpencerData): T = {
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

/*
case class Count[T <: SpencerAnalyser[RDD[Any]]](inner: T) extends SpencerAnalyser[Long] {
  override def analyse(implicit g: SpencerData): Long = {
    inner.analyse.count()
  }

  override def pretty(result: Long): String = this.toString+": "+result
}
*/

case class LifeTime(inner: VertexIdAnalyser) extends SpencerAnalyser[RDD[(VertexId, (Long, Long))]] {

  override def analyse(implicit g: SpencerData): RDD[(VertexId, (Long, Long))] = {
    //FIXME: uses collect
    g.db.getTable("objects")
        .select("id", "firstusage", "lastusage")
        .where("id IN ?", inner.analyse.collect().toList)
        .map(row => (row.getLong("id"), (row.getLong("firstusage"), row.getLong("lastusage"))))
  }

  override def pretty(result: RDD[(VertexId, (Long, Long))]): String = {
    "Lifetimes:\n\t"+result.collect().mkString(", ")
  }

  override def explanation(): String = "shows the first and last times objects were used"
}

case class ObjsByClass() extends SpencerAnalyser[RDD[(String, Iterable[VertexId])]] {
  override def analyse(implicit g: SpencerData): RDD[(String, Iterable[VertexId])] = {
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

/*
case class Mapped[T, U](inner: SpencerAnalyser[T], f: T => U) extends SpencerAnalyser[U] {
  override def analyse(implicit g: SpencerData): U = {
    f(inner.analyse)
  }

  override def pretty(result: U): String = this.toString+": "+result
}
*/

case class Collect[T](inner : SpencerAnalyser[RDD[T]]) extends SpencerAnalyser[Array[T]] {
  override def analyse(implicit g: SpencerData): Array[T] = {
    inner.analyse.collect()
  }

  override def pretty(result: Array[T]): String = {
    this.toString+":\n\t"+result.mkString("[ ", ", ", " ]")
  }

  override def explanation(): String = inner.explanation()
}

// Tabulate[String, AllClasses, Nothing, InstanceOfClass] = Tabulate(AllClasses(), InstanceOfClass)
// I String
// J Ana[RDD[String]]
// U Long
// V Ana[RDD[VertexId]]
// f String => Instance
//case class TabulateRDD[I, J <: SpencerAnalyser[RDD[I]], K]
//(inner : J, f: I => SpencerAnalyser[RDD[K]]) extends SpencerAnalyser[Array[(I, RDD[K])]] {
//  override def analyse(implicit g: SpencerData): Array[(I, RDD[K])] = {
//    val innerRet: Array[I] = inner.analyse(g).collect()
//    innerRet.map(x => (x, f(x).analyse))
//  }
//
//  override def pretty(result: Array[(I,RDD[K])]): String = {
//    val collected = result.map({case (i, k) => (i, k.collect())})
//    val sorted = if (collected.length < 1000) collected.sortBy(_._2.length * -1) else collected
//    sorted
//      .map({case (i, k) =>
//        k.mkString(i+"\t->\t [ ", ", ", " ]")
//      }).mkString("\n")
//  }
//
//  override def explanation(): String = "TabulateRDD"
//}

/*
case class Tabulate[I, J <: SpencerAnalyser[RDD[I]], K]
(inner : J, f: I => SpencerAnalyser[K]) extends SpencerAnalyser[RDD[(I, K)]] {
  override def analyse(implicit g: SpencerData): RDD[(I, K)] = {
    g.db.sc.parallelize(inner.analyse(g).collect().map(x => (x, f(x).analyse)))
  }

  override def pretty(result: RDD[(I,K)]): String = {
    result.collect()
      .map({case (i, k) =>
        i+"\t->\t"+k.toString
      }).mkString("\n")
  }
}
*/

/*
case class ClassProperty(prop: SpencerAnalyser[RDD[VertexId]]) extends SpencerAnalyser[RDD[String]] {
  override def analyse(implicit g: SpencerData): RDD[String] = {
    val propObjs = prop.analyse.collect().toSet
    val classVsObjs = g.db.getTable("objects")
      .groupBy(_.getStringOption("klass"))
      .filter(_._1.nonEmpty)
      .map({case (someKlass, objs) => (someKlass.get, objs)})
    classVsObjs.filter({
      case (klass, objs) =>
        val objsSet: Set[Long] = objs.flatMap(_.getLongOption("id")).toSet
        objsSet.subsetOf(propObjs)
    }).map(_._1)

  }

  override def pretty(result: RDD[String]): String = {
    this.toString+":\n\t"+SpencerAnalyserUtil.rddToString(result)
  }
}

object DeeplyImmutableClass {
  def apply() : SpencerAnalyser[RDD[String]] =
    ClassProperty(Deeply(ImmutableObj()))
}
*/

case class GroupByClass(inner: VertexIdAnalyser) extends SpencerAnalyser[RDD[(Option[String], Iterable[VertexId])]] {
  override def analyse(implicit g: SpencerData): RDD[(Option[String], Iterable[VertexId])] = {
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

case class PerClass[U: ClassTag](f : (Option[String], Iterable[VertexId]) => Option[U]) extends SpencerAnalyser[RDD[U]] {
  override def analyse(implicit g: SpencerData): RDD[U] = {
    val innerObjs: Set[VertexId] = Obj().analyse.collect().toSet[VertexId]
    val grouped: RDD[(Option[String], Iterable[VertexId])] =
      GroupByClass(Obj()).analyse
    if (classTag[U].toString.contains("RDD") /* hack */) {
      grouped.flatMap({case (loc, it) => f(loc, it)})
    } else {
      g.db.sc.parallelize(grouped.collect().flatMap({case (loc, it) => f(loc, it)}))
    }
  }

  override def pretty(result: RDD[U]): String = {
    result.collect().mkString(this.toString+":\n\t - ", "\n\t - ", "")
  }

  override def explanation(): String = "grouped by class, then mapped"
}

case class PerAllocationSite[U: ClassTag](f : ((Option[String], Option[Long]), Iterable[VertexId]) => Option[U]) extends SpencerAnalyser[RDD[U]] {
  override def analyse(implicit g: SpencerData): RDD[U] = {
    val innerObjs: Set[VertexId] = Obj().analyse.collect().toSet[VertexId]
    val grouped: RDD[((Option[String], Option[Long]), Iterable[VertexId])] =
      GroupByAllocationSite(Obj()).analyse
    if (classTag[U].toString.contains("RDD") /* hack */) {
      grouped.flatMap({case (loc, it) => f(loc, it)})
    } else {
      g.db.sc.parallelize(grouped.collect().flatMap({case (loc, it) => f(loc, it)}))
    }
  }

  override def pretty(result: RDD[U]): String = {
    result.collect().mkString(this.toString+":\n\t - ", "\n\t - ", "")
  }

  override def explanation(): String = "grouped by allocation sites and then mapped"
}

object ProportionPerClass {
  def apply(inner: VertexIdAnalyser)(implicit g: SpencerData) : SpencerAnalyser[RDD[(Option[String], (Int, Int))]] = {
    val innerSet = inner.analyse.collect().toSet
    PerClass({case (loc, iter) => {
      val instances: Set[VertexId] = iter.toSet
      Some((loc, ((instances intersect innerSet).size, instances.size)))
    }})
  }
}

object ProportionPerAllocationSite {
  def apply(inner: VertexIdAnalyser)(implicit g: SpencerData) : SpencerAnalyser[RDD[((Option[String], Option[Long]), (Int, Int))]] = {
    val innerSet = inner.analyse.collect().toSet
    PerAllocationSite({case (loc, iter) => {
      val allocSiteObjs: Set[VertexId] = iter.toSet
      Some((loc, ((allocSiteObjs intersect innerSet).size, allocSiteObjs.size)))
    }})
  }
}

case class ConnectedComponent(marker: VertexId, members: Array[VertexId]) {
  override def toString(): String = {
    "ConnectedComponent("+marker+", "+members.mkString("{", ",", "}")+")"
  }
}

case class ConnectedComponents(minSize: Int) extends SpencerAnalyser[RDD[ConnectedComponent]] {
  def analyse(implicit g: SpencerData): RDD[ConnectedComponent] = {
    val graph: Graph[ObjDesc, EdgeDesc] = g.graph.cache()
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
    val db: SpencerDB = new SpencerDB("test")
    db.connect()

    val watch: Stopwatch = Stopwatch.createStarted()
    implicit val g: SpencerData = SpencerGraphImplicits.spencerDbToSpencerGraph(db)

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


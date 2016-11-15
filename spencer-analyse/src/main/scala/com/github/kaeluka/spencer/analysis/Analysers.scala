package com.github.kaeluka.spencer.analysis

import java.io._
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import com.datastax.spark.connector.CassandraRow
import com.github.kaeluka.spencer.analysis.EdgeKind.EdgeKind
import com.github.kaeluka.spencer.tracefiles.SpencerDB
import com.google.common.base.Stopwatch
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD

import scala.language.implicitConversions
import scala.reflect.{ClassTag, _}
import org.objectweb.asm.{ClassReader, ClassVisitor}
import org.objectweb.asm.util.TraceClassVisitor

trait SpencerAnalyser[T] {
  def analyse(implicit g: SpencerData) : T
  def pretty(result: T): String
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

case class Klass() extends SpencerAnalyser[RDD[String]] {
  override def analyse(implicit g: SpencerData): RDD[String] = {
    g.db.getTable("objects")
      .select("klass")
      .map(_.getStringOption("klass")).filter(_.nonEmpty).map(_.get)
      .distinct()
  }

  override def pretty(result: RDD[String]): String = result.collect().mkString(this.toString+":\n\t { ", ", ", " }")
}

case class Snapshotted[T: ClassTag](inner : SpencerAnalyser[RDD[T]]) extends SpencerAnalyser[RDD[T]] {
  override def analyse(implicit g: SpencerData): RDD[T] = {
    if (inner.isInstanceOf[Snapshotted[_]]) {
      inner.analyse
    } else {
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
        val resultSerialised = snapshots .take(1)(0).getBytes("result")
        println(s"snapshot for query $queryString: reading ${resultSerialised.array().length} bytes")
        val ois: ObjectInputStream = new ObjectInputStream(new ByteArrayInputStream(resultSerialised.array()))
        g.db.sc.parallelize(ois.readObject().asInstanceOf[Array[T]])
      }
    }
  }

  override def pretty(result: RDD[T]): String = inner.pretty(result)

  override def toString: String = inner.toString
}

object AnalyserImplicits {
  implicit def toVertexIdAnalyser(a: SpencerAnalyser[RDD[VertexId]]): VertexIdAnalyser = {
    VertexIdAnalyser(a)
  }

  implicit def toRddAnalyser[T](a: SpencerAnalyser[RDD[T]]) : RddAnalyser[T] = {
    RddAnalyser(a)
  }
}

case class VertexIdAnalyser(inner: SpencerAnalyser[RDD[VertexId]]) extends SpencerAnalyser[RDD[Long]] {
  override def analyse(implicit g: SpencerData): RDD[VertexId] = {
    inner.analyse
  }

  override def pretty(result: RDD[VertexId]): String = {
    inner.pretty(result)
  }

  override def toString: String = inner.toString

  def and(other: VertexIdAnalyser) : SpencerAnalyser[RDD[VertexId]] = {
    new SpencerAnalyser[RDD[VertexId]] {
      override def analyse(implicit g: SpencerData): RDD[VertexId] = {
        inner.analyse.intersection(other.analyse)
      }

      override def pretty(result: RDD[VertexId]): String = {
        result
          .collect()
          .mkString(
            "objects that are both "+inner.toString+" and "+other.toString+":\n\t[ "
            , ", "
            , "] ")
      }

      override def toString: String = "(("+inner.toString+") and ("+other.toString+"))"
    }
  }

  def or(other: VertexIdAnalyser) : SpencerAnalyser[RDD[VertexId]] = {
    new SpencerAnalyser[RDD[VertexId]] {
      override def analyse(implicit g: SpencerData): RDD[VertexId] = {
        inner.analyse.union(other.analyse)
      }

      override def pretty(result: RDD[VertexId]): String = {
        result
          .collect()
          .mkString(
            "objects that are both "+inner.toString+" and "+other.toString+":\n\t[ "
            , ", "
            , "] ")
      }

      override def toString: String = "(("+inner.toString+") and ("+other.toString+"))"
    }
  }

  def vs(other: VertexIdAnalyser) : SpencerAnalyser[Map[(Boolean, Boolean), RDD[VertexId]]] = {
    new SpencerAnalyser[Map[(Boolean, Boolean), RDD[VertexId]]] {
      override def analyse(implicit g: SpencerData): Map[(Boolean, Boolean), RDD[VertexId]] = {
        val allRes: RDD[VertexId] = Obj().analyse.cache()
        val innerRes: RDD[VertexId] = inner.analyse
        val innerNotRes: RDD[VertexId] = allRes.subtract(innerRes)
        val otherRes: RDD[VertexId] = other.analyse
        val otherNotRes: RDD[VertexId] = allRes.subtract(otherRes)

        Map() +
          ((true,  true)  -> innerRes   .intersection(otherRes)) +
          ((true,  false) -> innerRes   .intersection(otherNotRes)) +
          ((false, true)  -> innerNotRes.intersection(otherRes)) +
          ((false, false) -> innerNotRes.intersection(otherNotRes))
      }

      override def pretty(result: Map[(Boolean, Boolean), RDD[VertexId]]): String = {
        var ret = ""
        for (k <- result.keysIterator) {
          ret = ret + (if (k._1) "    " else "not ")+inner.toString+(if (k._2) ",     " else ", not ")+other.toString+": "+SpencerAnalyserUtil.rddToString(result(k))+"\n"
        }
        ret
      }
    }
  }
}

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
}

case class WithMetaInformation(inner: SpencerAnalyser[RDD[VertexId]]) extends SpencerAnalyser[RDD[(Long, Option[String], Option[String])]] {

  override def analyse(implicit g: SpencerData): RDD[(Long, Option[String], Option[String])] = {
    val matchingIDs: RDD[VertexId] = inner.analyse(g)

//    matchingIDs.
    g.db.getTable("objects")
      .where("id IN ?", matchingIDs.collect().toList)
      .select("id", "klass", "allocationsitefile", "allocationsiteline")
      .map(row =>
        (
          row.getLong("id"),
          row.getStringOption("klass"),
          row.getStringOption("allocationsitefile").flatMap(file => row.getLongOption("allocationsiteline").map(file+":"+_))))
  }

  override def pretty(result: RDD[(Long, Option[String], Option[String])]): String = {

    "Satisfying "+inner+":\n\t"+
      result.collect().mkString("\n")
  }

}

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

case class ImmutableObj() extends SpencerAnalyser[RDD[VertexId]] {

  override def analyse(implicit g: SpencerData): RDD[VertexId] = {
    val objects: RDD[VertexId] = Obj().analyse

    val uses =
      g.db.getTable("uses")
        .where("callee > 0")

    val written =
      (uses.where("kind = 'modify'") ++ uses.where("kind = 'fieldstore'"))
        .filter(_.getString("method") != "<init>")
        .map(_.getLong("callee")).distinct()

    objects.subtract(written).setName("all objects minus mutables")
  }

  override def pretty(result: RDD[VertexId]): String = {
    result.collect().mkString(this.toString+":\n\t[ ", ", ", " ]")
  }
}

case class StationaryObj() extends SpencerAnalyser[RDD[VertexId]] {
  override def analyse(implicit g: SpencerData): RDD[VertexId] = {
    g.db.getTable("uses")
      .groupBy(_.getLong("callee").asInstanceOf[VertexId])
      .filter({
        case (callee, events) =>
          var hadRead = false
          var res : Option[VertexId] = Some(callee)
          val it: Iterator[CassandraRow] = events.iterator
          while (it.hasNext && res.nonEmpty) {
            val kind = it.next().getString("kind")
            if (hadRead) {
              if (kind == "fieldstore" || kind == "modify") {
                res = None
              }
            } else {
              if (kind == "fieldload" || kind == "read") {
                hadRead = true
              }
            }
          }
          res.nonEmpty
      })
      .map(_._1)
  }

  override def pretty(result: RDD[VertexId]): String = {
    result.collect().mkString(this.toString+":\n\t[ ", ", ", " ]")
  }
}

case class Obj() extends SpencerAnalyser[RDD[VertexId]] {

  override def analyse(implicit g: SpencerData): RDD[VertexId] = {
    g.db.getTable("objects").filter(_.getLong("id") > 4).map(_.getLong("id")).cache()
  }

  override def pretty(result: RDD[VertexId]): String = {
    "all objects:\n\t"+result.collect().mkString(", ")
  }
}

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
}

case class Count[T <: SpencerAnalyser[RDD[Any]]](inner: T) extends SpencerAnalyser[Long] {
  override def analyse(implicit g: SpencerData): Long = {
    inner.analyse.count()
  }

  override def pretty(result: Long): String = this.toString+": "+result
}

case class AllocatedAt(allocationSite: (Option[String], Option[Long])) extends SpencerAnalyser[RDD[VertexId]] {

  override def analyse(implicit g: SpencerData): RDD[VertexId] = {
    g.db.getTable("objects")
      .filter(row =>
        allocationSite ==
          (row.getStringOption("allocationsitefile")
            ,row.getLongOption("allocationsiteline")))
      .map(_.getLong("id"))
  }

  override def pretty(result: RDD[VertexId]): String = {
    "Allocated at "+allocationSite+":\n\t"+result.collect().mkString(", ")
  }

  override def toString() : String = {
    "AllocatedAt("+allocationSite._1.getOrElse("<any file>")+":"+allocationSite._2.map(_.toString).getOrElse("<any line>")+")"
  }
}

case class Lifetime(inner: SpencerAnalyser[RDD[VertexId]]) extends SpencerAnalyser[RDD[(VertexId, (Long, Long))]] {

  override def analyse(implicit g: SpencerData): RDD[(VertexId, (Long, Long))] = {
    g.db.getTable("objects")
        .select("id", "firstusage", "lastusage")
        .where("id IN ?", inner.analyse.collect().toList)
        .map(row => (row.getLong("id"), (row.getLong("firstusage"), row.getLong("lastusage"))))
  }

  override def pretty(result: RDD[(VertexId, (Long, Long))]): String = {
    "Lifetimes:\n\t"+result.collect().mkString(", ")
  }
}

case class InstanceOfClass(klassName: String) extends SpencerAnalyser[RDD[VertexId]] {

  def this(klass: Class[_]) =
    this(klass.getName)

  override def analyse(implicit g: SpencerData): RDD[VertexId] = {
    g.db.getTable("objects").filter(_.getStringOption("klass").contains(klassName)).map(_.getLong("id"))
  }

  override def pretty(result: RDD[VertexId]): String = {
    "instances of "+klassName+":\n\t"+result.collect().mkString(", ")
  }
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
}

case class Mapped[T, U](inner: SpencerAnalyser[T], f: T => U) extends SpencerAnalyser[U] {
  override def analyse(implicit g: SpencerData): U = {
    f(inner.analyse)
  }

  override def pretty(result: U): String = this.toString+": "+result
}

case class Collect[T](inner : SpencerAnalyser[RDD[T]]) extends SpencerAnalyser[Array[T]] {
  override def analyse(implicit g: SpencerData): Array[T] = {
    inner.analyse.collect()
  }

  override def pretty(result: Array[T]): String = {
    this.toString+":\n\t"+result.mkString("[ ", ", ", " ]")
  }
}

// Tabulate[String, AllClasses, Nothing, InstanceOfClass] = Tabulate(AllClasses(), InstanceOfClass)
// I String
// J Ana[RDD[String]]
// U Long
// V Ana[RDD[VertexId]]
// f String => Instance
case class TabulateRDD[I, J <: SpencerAnalyser[RDD[I]], K]
(inner : J, f: I => SpencerAnalyser[RDD[K]]) extends SpencerAnalyser[Array[(I, RDD[K])]] {
  override def analyse(implicit g: SpencerData): Array[(I, RDD[K])] = {
    val innerRet: Array[I] = inner.analyse(g).collect()
    innerRet.map(x => (x, f(x).analyse))
  }

  override def pretty(result: Array[(I,RDD[K])]): String = {
    val collected = result.map({case (i, k) => (i, k.collect())})
    val sorted = if (collected.length < 1000) collected.sortBy(_._2.length * -1) else collected
    sorted
      .map({case (i, k) =>
        k.mkString(i+"\t->\t [ ", ", ", " ]")
      }).mkString("\n")
  }
}

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

case class IsNot(inner: SpencerAnalyser[RDD[VertexId]]) extends SpencerAnalyser[RDD[VertexId]] {
  override def analyse(implicit g: SpencerData): RDD[VertexId] = {
    Obj().analyse.subtract(inner.analyse)
  }

  override def pretty(result: RDD[VertexId]): String = {
    result.collect().mkString("not "+inner.toString+":\n\t[ ", ", ", " ]")
  }
}

case class Named[T](inner: SpencerAnalyser[T], name: String) extends SpencerAnalyser[T] {
  override def analyse(implicit g: SpencerData): T = inner.analyse

  override def pretty(result: T): String = inner.pretty(result)

  override def toString: String = name
}

case object MutableObj {
  def apply(): SpencerAnalyser[RDD[VertexId]] = Named(IsNot(ImmutableObj()), "Mutable()")
}

case class Deeply(inner: SpencerAnalyser[RDD[VertexId]]) extends SpencerAnalyser[RDD[VertexId]] {
  override def analyse(implicit g: SpencerData): RDD[VertexId] = {
    val allObjs = Obj().analyse.cache()
    val negativeRoots = allObjs.subtract(inner.analyse)
    val reachingNegativeRoots = ConnectedWith(Const(negativeRoots), reverse = true, edgeFilter = Some(_ == EdgeKind.FIELD))
    allObjs.subtract(reachingNegativeRoots.analyse)
  }

  override def pretty(result: RDD[VertexId]): String = {
    this.toString+":\n\t"+SpencerAnalyserUtil.rddToString(result)
  }
}

//case class PerClass(prop: SpencerAnalyser[RDD[VertexId]]) extends SpencerAnalyser[RDD[String]] {
//  override def analyse(implicit g: SpencerData): RDD[String] = {
//    val propObjs = prop.analyse
//    val table: Array[(String, RDD[VertexId])] = TabulateRDD(Obj(), InstanceOfClass).analyse
//    val passed = table.flatMap({
//      case (klass, instances) =>
//        if (instances.subtract(propObjs).isEmpty()) {
//          Some(klass)
//        } else {
//          None
//        }
//    })
//    g.db.sc.parallelize(passed)
//  }
//
//  override def pretty(result: RDD[String]): String = {
//    this.toString
//  }
//}

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

case class GroupByClass(inner: SpencerAnalyser[RDD[VertexId]]) extends SpencerAnalyser[RDD[(Option[String], Iterable[VertexId])]] {
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
}
case class GroupByAllocationSite(inner: SpencerAnalyser[RDD[VertexId]]) extends SpencerAnalyser[RDD[((Option[String], Option[Long]), Iterable[VertexId])]] {
  override def analyse(implicit g: SpencerData): RDD[((Option[String], Option[Long]), Iterable[VertexId])] = {
    val innerCollected = inner.analyse.collect()
    g.db.getTable("objects")
      .select("allocationsitefile", "allocationsiteline", "id")
      .where("id IN ?", innerCollected.toList)
      .map(row => (row.getStringOption("allocationsitefile"), row.getLongOption("allocationsiteline"), row.getLong("id")))
      .map({case (row, line, id) =>
        (
          (
            row match {
              case Some(r) =>
                if (r.startsWith("<"))
                  None
                else
                  Some(r)
              case other => other
            },
            line match {
              case Some(-1) => None
              case other => other
            })
          , id
          )
      })
      .groupBy(_._1)
      .map({case (klass, iter) => (klass, iter.map(_._2))})
  }

  override def pretty(result: RDD[((Option[String], Option[Long]), Iterable[VertexId])]): String = {
    val collected =
      (if (result.count() < 1000) {
        result.sortBy(_._2.size*(-1))
      } else {
        result
      }).collect()

    this.toString+":\n"+
      collected.map({
        case (allocationSite, instances) => {
          val size = instances.size
          allocationSite+"\t-\t"+(if (size > 50) {
            instances.take(50).mkString("\t"+size+" x - [ ", ", ", " ... ]")
          } else {
            instances.mkString("\t"+size+" x - [ ", ", ", " ]")
          })
        }
      }).mkString("\n")
  }
}

case class ObjWithInstanceCountAtLeast(n : Int) extends SpencerAnalyser[RDD[VertexId]] {
  override def analyse(implicit g: SpencerData): RDD[VertexId] = {
    ObjsByClass().analyse
      .filter(_._2.size >= n)
      .flatMap(_._2)
  }

  override def pretty(result: RDD[VertexId]): String = {
    this.toString+":\n\t"+SpencerAnalyserUtil.rddToString(result)
  }
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
}

object ProportionPerClass {
  def apply(inner: SpencerAnalyser[RDD[VertexId]])(implicit g: SpencerData) : SpencerAnalyser[RDD[(Option[String], (Int, Int))]] = {
    val innerSet = inner.analyse.collect().toSet
    PerClass({case (loc, iter) => {
      val instances: Set[VertexId] = iter.toSet
      Some((loc, ((instances intersect innerSet).size, instances.size)))
    }})
  }
}

object ProportionPerAllocationSite {
  def apply(inner: SpencerAnalyser[RDD[VertexId]])(implicit g: SpencerData) : SpencerAnalyser[RDD[((Option[String], Option[Long]), (Int, Int))]] = {
    val innerSet = inner.analyse.collect().toSet
    PerAllocationSite({case (loc, iter) => {
      val allocSiteObjs: Set[VertexId] = iter.toSet
      Some((loc, ((allocSiteObjs intersect innerSet).size, allocSiteObjs.size)))
    }})
  }
}

case class ConstSeq[T: ClassTag](value: Seq[T]) extends SpencerAnalyser[RDD[T]] {
  override def analyse(implicit g: SpencerData): RDD[T] = {
    g.db.sc.parallelize(value)
  }

  override def pretty(result: RDD[T]): String = {
    value.mkString("[ ", ", ", " ]")
  }
}

case class Const[T](value: T) extends SpencerAnalyser[T] {
  override def analyse(implicit g: SpencerData): T = value

  override def pretty(result: T): String = this.toString
}

case class ConnectedWith(roots: SpencerAnalyser[RDD[VertexId]]
                         , reverse : Boolean = false
                         , edgeFilter : Option[EdgeKind => Boolean] = None) extends SpencerAnalyser[RDD[VertexId]] {

  override def analyse(implicit g: SpencerData): RDD[VertexId] = {
    val rootsCollected: Array[VertexId] = roots.analyse.collect()

    val empty = Set().asInstanceOf[Set[VertexId]]
    val mappedGraph = g.graph
      .mapVertices({ case (vertexId, objDesc) =>
          rootsCollected.contains(vertexId)
      })

    val subgraph = edgeFilter match {
      case Some(epred) => mappedGraph.subgraph(epred = triplet => epred(triplet.attr.kind))
      case None => mappedGraph
    }

    val directionCorrectedGraph = if (reverse) subgraph.reverse else subgraph

    val computed = directionCorrectedGraph
      .pregel(
        initialMsg = false
      )(
        vprog = {
          case (vertexId, isReached, reachedNow) =>
            isReached || reachedNow
        },
        sendMsg =  triplet => {
          if (!triplet.dstAttr && triplet.srcAttr) {
            Iterator((triplet.dstId, true))
          } else {
            Iterator.empty
          }
        },
        mergeMsg = _ && _)
    computed
      .vertices
      .filter(_._2)
      .map(_._1)

    //    g.db.sc.parallelize(res.toSeq)
  }

  override def pretty(result: RDD[VertexId]): String = {
    "Reachable from roots "+result.collect().mkString("[", ",", "]")+":\n\t"+result.collect().mkString("[", ", ", "]")
  }
}

object Scratch extends App {

  run

  def run: Unit = {
    val db: SpencerDB = new SpencerDB("test")
    db.connect()

    val watch: Stopwatch = Stopwatch.createStarted()
    implicit val g: SpencerData = SpencerGraphImplicits.spencerDbToSpencerGraph(db)

    val query =
    //    ProportionPerAllocationSite(Deeply(MutableObj()) and ObjWithInstanceCountAtLeast(10))
    //    InstanceOfClass("Foo") vs MaxInDegree(MaxInDegree.Unique, InDegreeSpec.HEAP)
    //    InstanceOfClass("Foo") vs (ImmutableObj() and ImmutableObj())
    //    InstanceOfClass("java.util.TreeSet")
    //    MutableObj()
    DeeplyImmutableClass()
    //    InDegree(InDegree.Aliased, InDegreeSpec.HEAP) vs InDegree(InDegree.Aliased, InDegreeSpec.STACK)
    //    Mutable() vs InDegree(InDegree.Aliased)
    //    (InstanceOfClass("Foo") and Mutable()) vs (InstanceOfClass("Foo") and InDegree(InDegree.Aliased))
    //    Apropos(90996)
    //    ProportionOutOf(InstanceOfClass("[C"), AllObjects())
    //    WithClassName(InDegree(_ > 1) and Mutable())
    //    ConnectedWith(InstanceOfClass("Foo")) and Mutable()
    //    Apropos(91947)
    //    WithClassName(ReachableFrom(InstanceOfClass("java.lang.String")))
    //    Tabulate(AllClasses(), (klass: String) => ProportionOutOf(Immutable(), InstanceOfClass(klass)))

    val res = Snapshotted(query).analyse

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

package com.github.kaeluka.spencer.analysis

import java.io._
import java.nio.charset.StandardCharsets

import com.github.kaeluka.spencer.PostgresSpencerDB
import com.google.common.base.Stopwatch
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.objectweb.asm.ClassReader
import org.objectweb.asm.util.TraceClassVisitor

import scala.language.implicitConversions

trait SpencerAnalyser[T] {
  def analyse(implicit g: PostgresSpencerDB) : T
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

case class SourceCode(klass: String) extends SpencerAnalyser[Option[String]] {
  override def analyse(implicit g: PostgresSpencerDB): Option[String] = {
    import g.sqlContext.implicits._
    val result =
      g.selectFrame("classdumps", s"SELECT bytecode FROM classdumps WHERE classname = '$klass'").as[Array[Byte]].rdd
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

object Scratch extends App {

  run

  def run(): Unit = {
    implicit val db: PostgresSpencerDB = new PostgresSpencerDB("test")
    db.connect()

    val watch: Stopwatch = Stopwatch.createStarted()

    val query =
//      InRefsHistory()
      QueryParser.parseObjQuery("Deeply(TinyObj())").right.get
//        ProportionPerAllocationSite(Deeply(MutableObj()) and ObjWithInstanceCountAtLeast(10))
//        InstanceOf("Foo") vs MaxInDegree(MaxInDegree.Unique, InDegreeSpec.HEAP)
//        InstanceOf("Foo") vs (ImmutableObj() and ImmutableObj())
//        InstanceOf("java.util.TreeSet")
//        MutableObj()
//        DeeplyImmutableClass()
//        InDegree(InDegree.Aliased, InDegreeSpec.HEAP) vs InDegree(InDegree.Aliased, InDegreeSpec.STACK)
//        Mutable() vs InDegree(InDegree.Aliased)
//        Apropos(28740)
//        ConnectedComponents(10)
//        ProportionOutOf(InstanceOf("[C"), AllObjects())
//        WithClassName(InDegree(_ > 1) and Mutable())
//        ConnectedWith(InstanceOf("Foo")) and Mutable()
//        Apropos(91947)
//        WithClassName(ReachableFrom(InstanceOf("java.lang.String")))
//        Tabulate(AllClasses(), (klass: String) => ProportionOutOf(Immutable(), InstanceOf(klass)))

    val res = query.analyse
    println("name: "+query.toString)
    println("result size: "+res.rdd.count())
    res.show(10)
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


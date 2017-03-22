package com.github.kaeluka.spencer.analysis

import java.io._
import java.nio.charset.StandardCharsets

import com.github.kaeluka.spencer.PostgresSpencerDB
import com.google.common.base.Stopwatch
import org.objectweb.asm.ClassReader
import org.objectweb.asm.util.TraceClassVisitor

import scala.language.implicitConversions

trait SpencerAnalyser[T] {
  def analyse(implicit g: PostgresSpencerDB) : T
  def explanation(): String
}

case class Apropos(id: Long) extends SpencerAnalyser[AproposData] {
  override def analyse(implicit g: PostgresSpencerDB): AproposData = {
    g.aproposObject(id)
  }

  override def explanation(): String = "the history of an object"
}

case class SourceCode(klass: String) extends SpencerAnalyser[Option[String]] {
  override def analyse(implicit g: PostgresSpencerDB): Option[String] = {
    val resultSet = g.runSQLQuery(s"SELECT bytecode FROM classdumps WHERE classname = '$klass'")
    val ret = if (resultSet.next()) {
      val bytecode = resultSet.getBytes("bytecode")
      assert(!resultSet.next, s"Have several ambiguous bytecodes for class $klass")
      val classreader = new ClassReader(bytecode)
      val baos = new ByteArrayOutputStream()
      val sw : PrintWriter = new PrintWriter(new PrintStream(baos))
      classreader.accept(new TraceClassVisitor(sw), ClassReader.EXPAND_FRAMES)
      Some(new String(baos.toByteArray, StandardCharsets.UTF_8))
    } else {
      None
    }
    resultSet.close()
    ret
  }

  override def explanation(): String = "shows the source code of a class"
}



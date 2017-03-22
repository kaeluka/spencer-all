package com.github.kaeluka.spencer.analysis

import com.github.kaeluka.spencer.PostgresSpencerDB

case class ObjWithMeta(oid: Long,
                       klass: Option[String],
                       allocationSite: Option[String],
                       firstusage: Long,
                       lastusage: Long,
                       thread: Option[String],
                       numFieldWrites: Long,
                       numFieldReads: Long,
                       numCalls: Long)

case class WithMetaInformation(inner: SpencerQuery) extends SpencerQuery {

  override def analyse(implicit g: PostgresSpencerDB) = {
    println("getting meta info")
    println("WARNING: GETTING ALL META INFO! USE JOINS!")
    super.analyse
  }

  def availableVariables : Map[String,String] = {
    Map(
      "klass"              -> "categorical",
      "allocationSite"     -> "categorical",
      "firstusage"         -> "numerical",
      "lastusage"          -> "numerical",
      "thread"             -> "categorical",
//      "numFieldWrites"     -> "numerical",
//      "numFieldReads"      -> "numerical",
      "numCalls"           -> "numerical"
    )
  }

  override def explanation(): String = inner.explanation()

  override def getVersion = { 0 }

  override def getSQLBlueprint = {
    """SELECT
      |  id,
      |  klass AS klass,
      |  allocationsitefile AS allocationsitefile,
      |  allocationsiteline AS allocationsiteline,
      |  firstusage AS firstusage,
      |  lastusage AS lastusage,
      |  COUNT(calls.callee) as numCalls,
      |  objects.thread as thread
      |FROM objects
      |LEFT OUTER JOIN calls ON calls.callee = objects.id
      |GROUP by objects.id
      |""".stripMargin
  }
}


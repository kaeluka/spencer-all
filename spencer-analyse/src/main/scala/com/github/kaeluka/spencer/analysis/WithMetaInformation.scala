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
      "numFieldWrites"     -> "numerical",
      "numFieldReads"      -> "numerical",
      "numCalls"           -> "numerical"
    )
  }

  override def explanation(): String = inner.explanation()

  override def getVersion = 2

  override def getSQLBlueprint = {
    """SELECT
      |  id,
      |  klass,
      |  allocationsitefile,
      |  allocationsiteline,
      |  firstusage,
      |  lastusage,
      |  numCalls,
      |  thread,
      |  COALESCE(numFieldReads,  0) AS numFieldReads,
      |  COALESCE(numFieldWrites, 0) AS numFieldWrites
      |FROM
      |  (SELECT
      |    id,
      |    klass AS klass,
      |    allocationsitefile,
      |    allocationsiteline,
      |    firstusage,
      |    lastusage,
      |    COUNT(calls.callee) as numCalls,
      |    objects.thread as thread
      |  FROM objects
      |  LEFT OUTER JOIN calls ON calls.callee = objects.id
      |  GROUP by objects.id) MAIN
      |LEFT OUTER JOIN
      |  (SELECT callee, COUNT(*) as numFieldWrites FROM uses
      |  WHERE kind = 'fieldstore'
      |  GROUP BY callee) WRITES  ON MAIN.id  = WRITES.callee
      |LEFT OUTER JOIN
      |  (SELECT callee, COUNT(*) as numFieldReads FROM uses
      |   WHERE kind = 'fieldload'
      |   GROUP BY callee) READS  ON MAIN.id  = READS.callee
      |""".stripMargin
  }
}


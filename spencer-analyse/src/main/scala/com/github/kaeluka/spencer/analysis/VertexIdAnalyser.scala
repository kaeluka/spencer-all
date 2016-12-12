package com.github.kaeluka.spencer.analysis

import com.datastax.driver.core.TableMetadata
import com.datastax.spark.connector.CassandraRow
import com.github.kaeluka.spencer.analysis.EdgeKind.EdgeKind
import com.google.common.base.Stopwatch
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

trait VertexIdAnalyser extends SpencerAnalyser[RDD[VertexId]] {

  def snapshotted() : VertexIdAnalyser = {
    SnapshottedVertexIdAnalyser(this)
  }

  override def pretty(result: RDD[VertexId]): String = {
    val N = result.count()
    val resString = if (N > 100) {
      result.take(100).mkString("{", ", ", " .. }")
    } else {
      result.collect().mkString("{", ", ", " }")
    }
    this.explanation()+":\n"+resString
  }
}

object And {
  def apply(vs: Seq[VertexIdAnalyser]) : VertexIdAnalyser = {
    println(s"AND of ${vs.mkString("[", ", ", "]")}")
    println(s"AND of ${vs.map(_.getClass.getName).mkString("[", ", ", "]")}")
    val vs_ = vs
      .filter(_.toString != "Obj()")
      .flatMap({
        case _And(innerVs) => innerVs
        case other => List(other)
      })
    println(s"AND vs_ == ${vs_.mkString("[", ", ", "]")}")
    assert(vs_.nonEmpty)
    if (vs_.size == 1) {
      println(s"returning ${vs_.head}")
      vs_.head
    } else {
      println(s"returning ${_And(vs_)}")
      _And(vs_)
    }
  }
}

case class _And(vs: Seq[VertexIdAnalyser]) extends VertexIdAnalyser {
  override def analyse(implicit g: SpencerData): RDD[VertexId] = {
    vs.map(_.analyse).reduce(_ intersection _)
  }

  override def explanation(): String = vs.map(_.explanation()).mkString(", and ")

  override def toString: String = vs.mkString("And(", " ", ")")
}

object Or {
  def apply(vs_ : Seq[VertexIdAnalyser]) : VertexIdAnalyser = {
    vs_.find(_.toString == "Obj()") match {
      case Some(q) => q
      case None => {
        val vs = vs_
          .flatMap({
            case Or_(innerVs) => innerVs
            case other => List(other)
          })
        Or_(vs)
      }
    }
  }
}

case class Or_(vs: Seq[VertexIdAnalyser]) extends VertexIdAnalyser {
  override def analyse(implicit g: SpencerData): RDD[VertexId] = {
    if (vs.contains(Obj())) {
      Obj().analyse
    } else {
      vs.map(_.analyse).reduce(_ union _).distinct()
    }
  }

  override def explanation(): String = vs.map(_.explanation()).mkString(", or ")

  override def toString: String = vs.mkString("Or(", " ", ")")
}

object SnapshottedVertexIdAnalyser {
  def apply(inner: VertexIdAnalyser) : VertexIdAnalyser = {
    if (inner.isInstanceOf[_SnapshottedVertexIdAnalyser]) {
      inner
    } else {
      _SnapshottedVertexIdAnalyser(inner)
    }
  }
}

case class _SnapshottedVertexIdAnalyser(inner : VertexIdAnalyser) extends VertexIdAnalyser {
  override def analyse(implicit g: SpencerData): RDD[VertexId] = {
    println(s"analysing snapshotted ${this.toString}, inner class: ${inner.getClass.getName}")
    assert(! inner.isInstanceOf[_SnapshottedVertexIdAnalyser])
    if (inner.isInstanceOf[_SnapshottedVertexIdAnalyser]) {
      inner.analyse
    } else {
      val keyspaceName: String = g.db.session.getLoggedKeyspace
      val keyspace = g.db.session.getCluster.getMetadata.getKeyspace(keyspaceName)
      val tblName = ("cache_"+ inner.toString.hashCode.toString+"_"+inner.getClass.getName.toString.hashCode).replace("-", "_")
      if (keyspace.getTable(tblName) == null) {

        println(s"caching ${inner.toString} in table $tblName")
        val watch: Stopwatch = Stopwatch.createStarted()
        val result = inner.analyse
        println("inner analysed! collecting...")
        val collectedResult = result.collect()
        println(s"analysis and collection took $watch")
        println("creating table...")
        watch.reset().start()
        try {
          g.db.session.execute(s"CREATE TABLE IF NOT EXISTS $keyspaceName.$tblName (id bigint PRIMARY KEY);")
          println("table created! analysing inner...")
          println(s"writing ${collectedResult.length} results to table..")
          var i = 0
          while (i < collectedResult.length) {
            g.db.session.execute(s"INSERT INTO $keyspaceName.$tblName (id) VALUES (?);", collectedResult(i) : java.lang.Long)
            i += 1
          }
          println(s"storing to table took $watch")
        } catch {
          case e: Throwable =>
            println(s"ignoring exception: ${e.getMessage}")
        }

        result
      } else {
        println(s"using cache for ${inner.toString}: $tblName")
        val ret: RDD[VertexId] = g.db.getTable(tblName).select("id").map(_.getLong("id"))
        println(s"returning ${ret.count} objects from cache")
        ret
      }
    }
  }

  override def snapshotted(): VertexIdAnalyser = this

  override def pretty(result: RDD[VertexId]): String = inner.pretty(result)

  override def toString: String = inner.toString

  override def explanation(): String = inner.explanation()
}

case class ImmutableObj() extends VertexIdAnalyser {

  override def analyse(implicit g: SpencerData): RDD[VertexId] = {
    val objects = Obj().analyse.collect()

    val uses =
      g.db.getTable("uses")
        .select("kind", "method", "callee")
        .where("callee > 4")

    val written =
      (uses.where("kind = 'modify'") ++ uses.where("kind = 'fieldstore'"))
        .filter(_.getString("method") != "<init>")
        .map(_.getLong("callee")).distinct()

    g.db.sc.parallelize(objects).subtract(written).setName("all objects minus mutables")
  }

  override def explanation(): String = "are never changed outside their constructor"
}

case class StationaryObj() extends VertexIdAnalyser {
  override def analyse(implicit g: SpencerData): RDD[VertexId] = {
    val writeAfterRead = g.db.getTable("uses")
      .select("callee", "method", "kind")
      .where("callee > 4")
      .filter(! _.getString("method").equals("<init>"))
      .groupBy(_.getLong("callee").asInstanceOf[VertexId])
      .filter({
        case (callee, events) =>
          var hadRead = false
          var res : Option[VertexId] = Some(callee)
          val it: Iterator[CassandraRow] = events.iterator
          while (it.hasNext && res.nonEmpty) {
            val nxt = it.next()
            val kind = nxt.getString("kind")
            if (nxt.getString("method") != "<init>") {
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
          }
          res.isEmpty
      })
      .map(_._1)

    Obj().analyse.subtract(writeAfterRead)
  }

  override def explanation(): String = "are never changed after being read from for the first time"
}

case class Obj() extends VertexIdAnalyser {

  override def analyse(implicit g: SpencerData): RDD[VertexId] = {
    g.db
      .getTable("objects")
      .select("id")
      .map(_.getLong("id"))
      .filter(_ >= 4)
  }

  override def explanation(): String = "were traced"
}

case class AllocatedAt(allocationSite: (String, Long)) extends VertexIdAnalyser {

  override def analyse(implicit g: SpencerData): RDD[VertexId] = {
    g.db.getTable("objects")
      .select("id", "allocationsitefile", "allocationsiteline")
      .where("allocationsitefile = ? AND allocationsiteline = ?", allocationSite._1, allocationSite._2)
      .select("id")
      //      .filter(row =>
      //        row.getStringOption("allocationsitefile").contains(allocationSite._1) &&
      //          row.getLongOption("allocationsiteline").contains(allocationSite._2))
      .map(_.getLong("id"))
  }

  override def toString() : String = {
    "AllocatedAt("+allocationSite._1+":"+allocationSite._2.toString+")"
  }

  override def explanation(): String = "were allocated at "+allocationSite._1+":"+allocationSite._2
}

case class InstanceOfClass(klassName: String) extends VertexIdAnalyser {

  def this(klass: Class[_]) =
    this(klass.getName)

  override def analyse(implicit g: SpencerData): RDD[VertexId] = {
    g.db
      .getTable("objects_per_class")
      .select("klass", "id")
      .where("klass = ?", klassName)
      .map(_.getLong("id"))
  }

  override def explanation(): String = "are instances of class "+klassName
}

case class IsNot(inner: VertexIdAnalyser) extends VertexIdAnalyser {
  override def analyse(implicit g: SpencerData): RDD[VertexId] = {
    Obj().analyse.subtract(inner.analyse)
  }

  override def explanation(): String = "not "+inner.explanation()
}

object Named {
  def apply(inner: VertexIdAnalyser, name: String) = {
    new Named(inner, name)
  }
}
case class Named(inner: VertexIdAnalyser, name: String, expl: String) extends VertexIdAnalyser {

  def this(inner: VertexIdAnalyser, name: String) =
    this(inner, name, inner.explanation())

  override def analyse(implicit g: SpencerData): RDD[VertexId] = inner.analyse

  override def pretty(result: RDD[VertexId]): String = inner.pretty(result)

  override def toString: String = name

  override def explanation(): String = this.expl
}

case object MutableObj {
  def apply(): VertexIdAnalyser = Named(IsNot(ImmutableObj()), "MutableObj()", "are changed outside of their constructor")
}

case class Deeply(inner: VertexIdAnalyser) extends VertexIdAnalyser {
  override def analyse(implicit g: SpencerData): RDD[VertexId] = {
    val allObjs = Obj().analyse.cache()
    val negativeRoots = allObjs.subtract(inner.analyse)
    val reachingNegativeRoots = ConnectedWith(Const(negativeRoots), reverse = true, edgeFilter = Some(_ == EdgeKind.FIELD))
    allObjs.subtract(reachingNegativeRoots.analyse)
  }

  override def explanation(): String = {
    inner.explanation()+", and the same is true for all reachable objects"
  }
}

case class ObjWithInstanceCountAtLeast(n : Int) extends VertexIdAnalyser {
  override def analyse(implicit g: SpencerData): RDD[VertexId] = {
    ObjsByClass().analyse
      .filter(_._2.size >= n)
      .flatMap(_._2)
  }

  override def explanation(): String = "created from classes with at least "+n+" instances in total"
}

case class ConstSeq(value: Seq[VertexId]) extends VertexIdAnalyser {
  override def analyse(implicit g: SpencerData): RDD[VertexId] = {
    g.db.sc.parallelize(value)
  }

  override def pretty(result: RDD[VertexId]): String = {
    value.mkString("[ ", ", ", " ]")
  }

  override def explanation(): String = "any of "+value.mkString("{", ", ", "}")
}

case class Const(value: RDD[VertexId]) extends VertexIdAnalyser {
  override def analyse(implicit g: SpencerData):RDD[VertexId] = value

  override def pretty(result: RDD[VertexId]): String = this.toString

  override def explanation(): String = "constant set "+value.toString
}

case class ConnectedWith(roots: VertexIdAnalyser
                         , reverse : Boolean = false
                         , edgeFilter : Option[EdgeKind => Boolean] = None) extends VertexIdAnalyser {

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

  override def explanation(): String = if (reverse) {
    "can reach any objects that "+roots.explanation()
  } else {
    "are reachable from objects that "+roots.explanation()
  }
}

case class TinyObj() extends VertexIdAnalyser {
  override def analyse(implicit g: SpencerData): RDD[VertexId] = {
    val withRefTypeFields = g.graph.triplets.filter(_.attr.kind == EdgeKind.FIELD).map(_.srcId).distinct()
    Obj().analyse.subtract(withRefTypeFields)
  }

  override def explanation(): String = "do not have or do not use reference type fields"
}


case class GroupByAllocationSite(inner: VertexIdAnalyser) extends SpencerAnalyser[RDD[((Option[String], Option[Long]), Iterable[VertexId])]] {
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
        case (allocationSite, instances) =>
          val size = instances.size
          allocationSite+"\t-\t"+(if (size > 50) {
            instances.take(50).mkString("\t"+size+" x - [ ", ", ", " ... ]")
          } else {
            instances.mkString("\t"+size+" x - [ ", ", ", " ]")
          })
      }).mkString("\n")
  }

  override def explanation(): String = "some objects, grouped by allocation site"
}


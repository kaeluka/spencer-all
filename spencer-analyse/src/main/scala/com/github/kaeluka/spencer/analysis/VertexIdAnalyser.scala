package com.github.kaeluka.spencer.analysis

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.ByteBuffer

import com.datastax.spark.connector.CassandraRow
import com.github.kaeluka.spencer.analysis.EdgeKind.EdgeKind
import com.google.common.base.Stopwatch
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

trait VertexIdAnalyser extends SpencerAnalyser[RDD[VertexId]] {

  def and(other: VertexIdAnalyser) : VertexIdAnalyser = {
    if (other.isInstanceOf[Obj]) {
      this
    } else if (this.isInstanceOf[Obj]) {
      other
    } else {
      new VertexIdAnalyser {
        override def analyse(implicit g: SpencerData): RDD[VertexId] = {
          this.analyse.intersection(other.analyse)
        }

        override def explanation(): String = VertexIdAnalyser.this.explanation() + ", and that " + other.explanation()

        override def toString: String = "And(" + VertexIdAnalyser.this.toString + " " + other.toString + ")"
      }
    }
  }

  def snapshotted() : VertexIdAnalyser = {
    SnapshottedVertexIdAnalyser(this)
  }

  def or(other: VertexIdAnalyser) : VertexIdAnalyser = {
    if (other.isInstanceOf[Obj]) {
      other
    } else if (this.isInstanceOf[Obj]) {
      this
    } else {
      new VertexIdAnalyser {
        override def analyse(implicit g: SpencerData): RDD[VertexId] = {
          this.analyse.union(other.analyse)
        }

        override def explanation(): String = VertexIdAnalyser.this.explanation() + ", or that " + other.explanation()

        override def toString: String = "And(" + VertexIdAnalyser.this.toString + " " + other.toString + ")"
      }
    }
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

case class SnapshottedVertexIdAnalyser(inner : VertexIdAnalyser) extends VertexIdAnalyser {
  override def analyse(implicit g: SpencerData): RDD[VertexId] = {
    if (inner.isInstanceOf[SnapshottedVertexIdAnalyser]) {
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
        val res = g.db.sc.parallelize(ois.readObject().asInstanceOf[Array[VertexId]])
        println(s"deserialising took $watch")
        res
      }
    }
  }

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


case class WithMetaInformation(inner: VertexIdAnalyser) extends SpencerAnalyser[RDD[(Long, Option[String], Option[String])]] {

  override def analyse(implicit g: SpencerData): RDD[(Long, Option[String], Option[String])] = {
    val matchingIDs: RDD[VertexId] = inner.analyse(g)

    //    matchingIDs.
    g.db.getTable("objects")
      .select("id", "klass", "allocationsitefile", "allocationsiteline")
      .where("id IN ?", matchingIDs.collect().toList)
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

  override def explanation(): String = inner.explanation()

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


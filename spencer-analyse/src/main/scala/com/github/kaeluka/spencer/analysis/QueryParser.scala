package com.github.kaeluka.spencer.analysis

import com.github.kaeluka.spencer.analysis.AnalyserImplicits._
import fastparse.all._
import fastparse.core.Parsed
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD

object QueryParser {

//  def complexObjQuery : P[SpencerAnalyser[RDD[VertexId]]] =
//    objQuery.rep(2, sep="and").map(_.reduce(_ and _)) | objQuery.rep(2, sep="or").map(_.reduce(_ or _)) | objQuery

  def objQuery: P[SpencerAnalyser[RDD[VertexId]]] =
    primitiveObjQuery | parameterisedObjQuery

  def connectedWith =
    P("ReachableFrom("~objQuery~")").map(x => Named(ConnectedWith(x), "ReachableFrom("+x.toString+")")) |
      P("CanReach("~objQuery~")").map(x => Named(ConnectedWith(x, reverse = true), "CanReach("+x.toString+")"))

  def deeply =
    P("Deeply("~objQuery~")").map(Deeply)

  def constSet =
    P("Set(" ~ number.rep(sep = " ").map(_.toSet) ~")").map(set => Named(ConstSeq(set.toSeq), "Set"))

  def number : P[Long] =
    ("-".? ~ CharIn('0' to '9')).rep(1).!.map(_.toLong)

  def instanceOfKlass =
    P("InstanceOfClass("~className~")")
   .map(InstanceOfClass)

  def className: P[String] = {
    P((CharIn('a' to 'z') | CharIn('A' to 'Z') | "." | "[" | "$" | ";").rep.!)
  }

  def allocatedAt =
    P("AllocatedAt("~location~")")
      .map(AllocatedAt)

  def location: P[(String, Long)] =
    P((CharIn('a' to 'z') | CharIn('A' to 'Z') | "_" | ".").rep(1).! ~ ":" ~ number)

  def bigOr =
    P("Or("~objQuery.rep(2, sep=" ")~")").map(
      xs => Named(xs.reduce(_ and _), xs.mkString("Or(", " ", ")")))

  def bigAnd =
    P("And("~objQuery.rep(2, sep=" ")~")").map(
      xs => Named(xs.reduce(_ and _), xs.mkString("And(", " ", ")")))

  def isNot =
    P("Not("~objQuery~")").map(IsNot)

  def parameterisedObjQuery : P[SpencerAnalyser[RDD[VertexId]]] =
    connectedWith | deeply | instanceOfKlass | allocatedAt | constSet | isNot | bigAnd | bigOr

//  def binaryOpObjQuery : P[SpencerAnalyser[RDD[VertexId]]] =
//    P(objQuery ~ " "~("and"|"or").! ~" " ~ objQuery).map({
//      case (l, "and", r) => l and r
//      case (l, "or", r)  => l or r
//    })

  def primitiveObjQuery : P[SpencerAnalyser[RDD[VertexId]]] =
    P(("MutableObj()"
      | "ImmutableObj()"
      | "StationaryObj()"
      | "UniqueObj()"
      | "HeapUniqueObj()"
      | "TinyObj()"
      | "StackBoundObj()"
      | "Obj()"
      | "PrimitiveObj()"
      ).!).map {
      case "MutableObj()" => Snapshotted(MutableObj())
      case "ImmutableObj()" => Snapshotted(ImmutableObj())
      case "StationaryObj()" => Snapshotted(StationaryObj())
      case "UniqueObj()" => Snapshotted(Named(MaxInDegree(MaxInDegree.Unique), "UniqueObj()", "have at most one active reference at each time"))
      case "HeapUniqueObj()" => Snapshotted(Named(MaxInDegree(MaxInDegree.Unique, InDegreeSpec.HEAP), "HeapUniqueObj()", "have at most one active heap reference at each time"))
      case "TinyObj()" => Snapshotted(TinyObj())
      case "StackBoundObj()" => Snapshotted(Named(MaxInDegree(MaxInDegree.None, InDegreeSpec.HEAP), "StackBoundObj()", "never escape to the heap"))
      case "Obj()" => Snapshotted(Obj())
    }

  def parseObjQuery(txt: String): Either[String, SpencerAnalyser[RDD[VertexId]]] = {
    val res: Parsed[SpencerAnalyser[RDD[VertexId]], Char, String] = objQuery.parse(txt.replace("%20", " "))
    res match {
      case Parsed.Success(value, _) => Right(Snapshotted(value))
      case Parsed.Failure(_, index, extra) =>
        Left("parsing failed :\n"+txt+"\n"+(" "*index)+"^\nrest: "+extra)
    }
  }

  def primitiveQueries(klass: String = "java.lang.String", allocationSite: String = "somefile:123") : List[SpencerAnalyser[RDD[VertexId]]] = {
    List("MutableObj()",
      "ImmutableObj()",
      "StationaryObj()",
      "UniqueObj()",
      "HeapUniqueObj()",
      "TinyObj()",
      "StackBoundObj()",
      "InstanceOfClass("+klass+")",
      "AllocatedAt("+allocationSite+")",
      "Obj()"
    )
      .map(QueryParser.parseObjQuery(_))
        .map(x => {
          assert(x.isRight, x+toString+" must be right!")
          x
        })
      .map(_.right.get)
  }

}

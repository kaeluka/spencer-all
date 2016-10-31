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
    P("ReachableFrom("~objQuery~")").map(ConnectedWith(_)) |
      P("CanReach("~objQuery~")").map(ConnectedWith(_, reverse = true))

  def deeply =
    P("Deeply("~objQuery~")").map(Deeply)

  def constSet =
    P("Set(" ~ number.rep(sep = " ").map(_.toSet) ~")").map(set => ConstSeq(set.toSeq))

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

  def location: P[(Option[String], Option[Long])] =
    P((CharIn('a' to 'z') | CharIn('A' to 'Z') | "_" | ".").rep(1).! ~ ":" ~ number)
    .map({case (file, line) => (Some(file), Some(line))})

  def bigOr =
    P("Or("~objQuery.rep(2, sep=" ")~")").map(_.reduce(_ and _))

  def bigAnd =
    P("And("~objQuery.rep(2, sep=" ")~")").map(_.reduce(_ and _)) | P(objQuery.rep(2, sep=" and ").map(_.reduce(_ and _)))

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
      | "UniqueObj()"
      | "Obj()"
      ).!).map {
      case "MutableObj()" => Snapshotted(MutableObj())
      case "ImmutableObj()" => Snapshotted(ImmutableObj())
      case "UniqueObj()" => Snapshotted(MaxInDegree(MaxInDegree.Unique))
      case "Obj()" => Snapshotted(Obj())
    }

  def parseObjQuery(txt: String): Either[String, SpencerAnalyser[RDD[VertexId]]] = {
    val res: Parsed[SpencerAnalyser[RDD[VertexId]], Char, String] = objQuery.parse(txt)
    res match {
      case Parsed.Success(value, _) => Right(value)
      case Parsed.Failure(_, index, extra) =>
        Left("parsing failed :\n"+txt+"\n"+(" "*index)+"^\nrest: "+extra)
    }
  }

}

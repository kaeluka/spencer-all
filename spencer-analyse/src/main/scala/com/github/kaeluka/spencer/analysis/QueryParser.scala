package com.github.kaeluka.spencer.analysis

import scala.util.parsing.combinator._
import fastparse.all._
import fastparse.core.Parsed
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD

import AnalyserImplicits._

object QueryParser {

  def objQuery: P[SpencerAnalyser[RDD[VertexId]]] =
    primitiveObjQuery | parameterisedObjQuery

  def connectedWith =
    P("ReachableFrom("~objQuery~")").map(ConnectedWith(_)) |
      P("CanReach("~objQuery~")").map(ConnectedWith(_, reverse = true))

  def deeply =
    P("Deeply("~objQuery~")").map(Deeply(_))

  def constSet =
    P("Set(" ~ (number.rep(sep = " ")).map(_.toSet) ~")").map(set => ConstSeq(set.toSeq))

  def number : P[Long] =
    ("-".? ~ CharIn('0' to '9')).rep(1).!.map(_.toLong)

  def instanceOfKlass =
    P("InstanceOfClass("~className~")")
   .map(InstanceOfClass(_))

  def className: P[String] = {
    P((CharIn('a' to 'z') | CharIn('A' to 'Z') | "." | "[" | "$").rep.!)
  }

  def isNot =
    P("IsNot("~objQuery~")").map(IsNot(_))

  def parameterisedObjQuery : P[SpencerAnalyser[RDD[VertexId]]] =
    connectedWith | deeply | instanceOfKlass | constSet | isNot

  def binaryOpObjQuery : P[SpencerAnalyser[RDD[VertexId]]] =
    P(objQuery ~ " "~("and"|"or").! ~" " ~ objQuery).map({
      case (l, "and", r) => l and r
      case (l, "or", r)  => l or r
    })

  def primitiveObjQuery : P[SpencerAnalyser[RDD[VertexId]]] =
    P(("MutableObj()"
      | "ImmutableObj()"
      | "Obj()"
      ).!).map(_ match {
      case "MutableObj()"     => Snapshotted(MutableObj())
      case "ImmutableObj()"   => Snapshotted(ImmutableObj())
      case "Obj()"            => Snapshotted(Obj())
    })

  def parseObjQuery(txt: String): Option[SpencerAnalyser[RDD[VertexId]]] = {
    val res: Parsed[SpencerAnalyser[RDD[VertexId]], Char, String] = objQuery.parse(txt)
    res match {
      case Parsed.Success(value, _) => Some(value)
      case _ => None
    }
  }

}

package com.github.kaeluka.spencer.analysis

import fastparse.all._
import fastparse.core.Parsed

object QueryParser {

  def objQuery: P[VertexIdAnalyser] =
    primitiveObjQuery | parameterisedObjQuery

  def connectedWith =
    P("ReachableFrom("~objQuery~")").map(x => Named(ConnectedWith(x), "ReachableFrom("+x.toString+")")) |
      P("CanReach("~objQuery~")").map(x => Named(ConnectedWith(x, reverse = true), "CanReach("+x.toString+")")) |
      P("HeapReachableFrom("~objQuery~")").map(x => Named(ConnectedWith(x), "HeapReachableFrom("+x.toString+")")) |
      P("CanHeapReach("~objQuery~")").map(x => Named(ConnectedWith(x, reverse = true), "CanHeapReach("+x.toString+")"))

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
    P("Or("~objQuery.rep(2, sep=" ")~")").map(xs => Or(xs))

  def bigAnd =
    P("And("~objQuery.rep(2, sep=" ")~")").map(xs => And(xs))

  def isNot =
    P("Not("~objQuery~")").map(q => Named(IsNot(q), s"Not(${q.toString})"))

  def parameterisedObjQuery : P[VertexIdAnalyser] =
    connectedWith | deeply | instanceOfKlass | allocatedAt | constSet | isNot | bigAnd | bigOr

  def primitiveObjQuery : P[VertexIdAnalyser] = {
    P(("MutableObj()"
      | "ImmutableObj()"
      | "StationaryObj()"
      | "UniqueObj()"
      | "HeapUniqueObj()"
      | "TinyObj()"
      | "StackBoundObj()"
      | "AgeOrderedObj()"
      | "ReverseAgeOrderedObj()"
      | "Obj()"
      ).!)
      .map {
        case "MutableObj()"           => MutableObj()
        case "ImmutableObj()"         => ImmutableObj()
        case "StationaryObj()"        => StationaryObj()
        case "UniqueObj()"            => MaxInDegree.UniqueObj()
        case "HeapUniqueObj()"        => MaxInDegree.HeapUniqueObj()
        case "TinyObj()"              => TinyObj()
        case "StackBoundObj()"        => MaxInDegree.StackBoundObj()
        case "AgeOrderedObj()"        => AgeOrderedObj()
        case "ReverseAgeOrderedObj()" => ReverseAgeOrderedObj()
        case "Obj()"                  => Obj()
      }
      .map(_.snapshotted())
  }

  def parseObjQuery(txt: String): Either[String, VertexIdAnalyser] = {
    val res: Parsed[VertexIdAnalyser, Char, String] = objQuery.parse(txt.replace("%20", " "))
    res match {
      case Parsed.Success(value, _) => Right(value.snapshotted())
      case Parsed.Failure(_, index, extra) =>
        Left("parsing failed :\n"+txt+"\n"+(" "*index)+"^\nrest: "+extra)
    }
  }

  def primitiveQueries(klass: String = "java.lang.String", allocationSite: String = "String.java:1933") : List[VertexIdAnalyser] = {
    List("MutableObj()",
      "ImmutableObj()",
      "StationaryObj()",
      "UniqueObj()",
      "HeapUniqueObj()",
      "TinyObj()",
      "StackBoundObj()",
      "AgeOrderedObj()",
      "ReverseAgeOrderedObj()",
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

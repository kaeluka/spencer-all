package com.github.kaeluka.spencer.analysis

import fastparse.all._
import fastparse.core.Parsed

object QueryParser {

  def objQuery: P[VertexIdAnalyser] =
    primitiveObjQuery | parameterisedObjQuery

  def connectedWith =
    P("ReferredFrom("~objQuery~")")       .map(ReferredFrom) |
      P("RefersTo("~objQuery~")")         .map(RefersTo) |
      P("HeapReferredFrom("~objQuery~")") .map(HeapReferredFrom) |
      P("HeapRefersTo("~objQuery~")")     .map(HeapRefersTo) |
      P("ReachableFrom("~objQuery~")")    .map(ReachableFrom) |
      P("CanReach("~objQuery~")")         .map(CanReach) |
      P("HeapReachableFrom("~objQuery~")").map(HeapReachableFrom) |
      P("CanHeapReach("~objQuery~")")     .map(CanHeapReach)

  def deeply =
    P("Deeply("~objQuery~")")
      .map(q => Named(Deeply(q), s"Deeply(${q.toString})")) |
      P("HeapDeeply("~objQuery~")")
        .map(q => Named(Deeply(q,
          edgeFilter = Some(EdgeKind.FIELD)), s"HeapDeeply(${q.toString})"))

  def constSet =
    P("Set(" ~ number.rep(sep = " ").map(_.toSet) ~")").map(set => Named(ConstSeq(set.toSeq), "Set"))

  def number : P[Long] =
    ("-".? ~ CharIn('0' to '9')).rep(1).!.map(_.toLong)

  def instanceOfKlass =
    (P("InstanceOf("~className~")") | P("InstanceOfClass("~className~")"))
   .map(InstanceOf)

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
    P("Not("~objQuery~")").map(q => Not(q))

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
      | "ThreadLocalObj()"
      | "Obj()"
      ).!)
      .map {
        case "MutableObj()"           => MutableObj()
        case "ImmutableObj()"         => ImmutableObj()
        case "StationaryObj()"        => StationaryObj()
        case "UniqueObj()"            => UniqueObj()
        case "HeapUniqueObj()"        => HeapUniqueObj()
        case "TinyObj()"              => TinyObj()
        case "StackBoundObj()"        => StackBoundObj()
        case "AgeOrderedObj()"        => AgeOrderedObj()
        case "ReverseAgeOrderedObj()" => ReverseAgeOrderedObj()
        case "ThreadLocalObj()"       => ThreadLocalObj()
        case "Obj()"                  => Obj()
      }
      .map(_.snapshotted())
  }

  def escape(txt: String): String = {
    txt
      .replace(" ", "%20")
      .replace("[", "%5B")
  }

  def unescape(txt: String): String = {
    txt
      .replace("%20", " ")
      .replace("%5B", "[")
  }

  def parseObjQuery(txt: String): Either[String, VertexIdAnalyser] = {
    val res: Parsed[VertexIdAnalyser, Char, String] = objQuery.parse(
      unescape(txt)
    )
    res match {
      case Parsed.Success(value, _) => Right(value.snapshotted())
      case Parsed.Failure(_, index, extra) =>
        Left("parsing failed :\n"+txt+"\n"+(" "*index)+"^\nrest: "+extra)
    }
  }

  def primitiveQueries(klass: String = "java.lang.String", allocationSite: String = "String.java:1933") : List[VertexIdAnalyser] = {
    List("MutableObj()",
      "ImmutableObj()",
//      "StationaryObj()",
      "UniqueObj()",
      "HeapUniqueObj()",
      "TinyObj()",
      "StackBoundObj()",
      "AgeOrderedObj()",
      "ReverseAgeOrderedObj()",
//      "ThreadLocalObj()",
      "InstanceOf("+klass+")",
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

  def wrapQueries(queryCombinator: String, queries: List[VertexIdAnalyser]): List[VertexIdAnalyser] = {
    //println(s"wrapping: ${queryCombinator}")
    queries
      .map(q => s"$queryCombinator(${q.toString})")
      .map(parseObjQuery)
      .map(_.right.get)
  }

  def seriesOfQueries() : List[VertexIdAnalyser] = {
    var ret: List[VertexIdAnalyser] = primitiveQueries()
    ret ++
      wrapQueries("CanReach", ret) ++
      wrapQueries("CanHeapReach", ret) ++
      wrapQueries("ReachableFrom", ret) ++
      wrapQueries("HeapReachableFrom", ret) ++
      wrapQueries("Deeply", ret) ++
      wrapQueries("HeapDeeply", ret)
  }

}

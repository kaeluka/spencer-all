package com.github.kaeluka.spencer.analysis
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class QueryParserTest extends FlatSpec with Matchers {

  "parse" should "return None on invalid inputs" in {
    QueryParser.parseObjQuery("") should be (None)
    QueryParser.parseObjQuery("Mutable(") should be (None)
    QueryParser.parseObjQuery("aaaa()") should be (None)
  }

  "parse" should "return something on valid inputs" in {
    val objQueries = List("ImmutableObj()"
      , "Obj()"
      , "ConnectedWith(ImmutableObj())"
      , "ConnectedWith(ImmutableObj(), reverse)"
      , "InstanceOfClass(java.lang.String)"
      , "MutableObj()"
      , "Deeply(ImmutableObj())"
      , "IsNot(MutableObj())"
      , "InstanceOfClass(java.lang.String) and MutableObj()"
    )
    for (elem <- objQueries) {
      QueryParser.parseObjQuery(elem) should not be empty
    }


//    , "Klass()"

  }
}

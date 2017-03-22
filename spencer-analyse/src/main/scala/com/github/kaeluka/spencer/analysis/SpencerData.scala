package com.github.kaeluka.spencer.analysis

case class ObjDesc(klass: Option[String])

case class EdgeDesc(fr: Option[Long], to: Option[Long], kind: EdgeKind.EdgeKind)

object EdgeKind extends Enumeration {
  type EdgeKind = Value
  val FIELDSTORE, FIELDLOAD, VARSTORE, VARLOAD, READ, MODIFY, FIELD, VAR = Value
  def fromRefsKind(s: String) : Value = {
    s match {
//      case "fieldstore" => FIELDSTORE
//      case "fieldload"  => FIELDLOAD
//      case "varstore"   => VARSTORE
//      case "varload"    => VARLOAD
//      case "read"       => READ
//      case "modify"     => MODIFY
      case "field"      => FIELD
      case "var"        => VAR
    }
  }

  def fromUsesKind(s: String) : Value = {
    s match {
      case "fieldstore" => FIELDSTORE
      case "fieldload"  => FIELDLOAD
      case "varstore"   => VARSTORE
      case "varload"    => VARLOAD
      case "read"       => READ
      case "modify"     => MODIFY
      //      case "field"      => FIELD
      //      case "var"        => VAR
    }
  }
}

object UseKind extends Enumeration {
  type UseKind = Value
  val READ, MODIFY = Value
  def fromEventKind(s: String) : Value = {
    s match {
      case "fieldstore" => MODIFY
      case "fieldload"  => READ
      case "varstore"   => READ
      case "varload"    => READ
      case "read"       => READ
      case "modify"     => MODIFY
      case "field"      => ???
      case "var"        => ???
    }
  }
}


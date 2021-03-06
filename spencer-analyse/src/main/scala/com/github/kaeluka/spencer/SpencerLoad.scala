package com.github.kaeluka.spencer

import java.nio.file.Paths

object SpencerLoad {

//  val defaultTracefile = "/Volumes/MyBook/tracefiles/pmd.small/tracefile"
//  val defaultTracefile = "/tmp/tracefile"
//  val defaultTracefile = "/tmp/tracefile.zip"
  val defaultTracefile = "/Users/stebr742/code/kaeluka/spencer-playground/tracefile"

  def main(args: Array[String]) {
    println(args.mkString(", "))

    val name = args
      .find(_.startsWith("name="))
      .map(_.replace("name=", ""))
      .getOrElse("test")

    val rest = args.filter(!_.contains("="))

    val path =
      if (rest.length != 1) {
        System.err.println("no tracefile given (or too many), defaulting to "+defaultTracefile)
        defaultTracefile
      } else {
        rest(0)
    }

    val oBytecodeDir = args
      .find(_.startsWith("bytecode-dir="))
      .map(_.replace("bytecode-dir=", ""))

    println("spencer loader starting...")
    println("loading "+path+" as '"+name+"'")

    val db = new PostgresSpencerDB(name)
    db.loadFrom(path, Paths.get(oBytecodeDir.getOrElse(Paths.get(path).getParent.toString+"/log/")))
    println("done")
    sys.exit(0)
  }
}

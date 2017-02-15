package com.github.kaeluka.spencer

import java.nio.file.Paths

object SpencerCache {

  val defaultTracefile = "/Users/stebr742/code/kaeluka/spencer-playground/tracefile"

  def main(args: Array[String]) {
    println(args.mkString(", "))

    val name = args
      .find(_.startsWith("name="))
      .map(_.replace("name=", ""))
      .getOrElse("test")

    println("spencer cache tool starting...")
    println(s"caching in database $name")

    val db = new PostgresSpencerDB(name, false)
    db.connect()
    if (args.contains("--replace")) {
      print("clearing all caches first..")
      db.clearCaches(name)
      println("done")
    }
    db.cacheQueries()
    println("caching done")
    sys.exit(0)
  }
}

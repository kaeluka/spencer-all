package com.github.kaeluka.spencer

import java.io._
import java.net.URLClassLoader
import java.util.zip.ZipEntry

import org.apache.commons.compress.archivers.zip.{ZipArchiveEntry, ZipFile}

import com.github.kaeluka.spencer.tracefiles.SpencerDB

object SpencerLoad {

//  val defaultTracefile = "/Volumes/MyBook/tracefiles/pmd.small/tracefile"
//  val defaultTracefile = "/tmp/tracefile"
  val defaultTracefile = "/tmp/tracefile.zip"

  def main(args: Array[String]) {
    println(args.mkString(", "))

    val name = args
      .find(_.startsWith("name="))
      .map(_.replace("name=", ""))
      .getOrElse("test")

    val rest = args.filter(!_.startsWith("name="))


    println((SpencerLoad.getClass().getClassLoader()).asInstanceOf[URLClassLoader].getURLs.mkString(",\n"))

    val path =
      if (rest.length != 1) {
        System.err.println("no tracefile given (or too many), defaulting to "+defaultTracefile)
        defaultTracefile
      } else {
        rest(0)
    }
    println("spencer cassandra loader starting...")
    println("loading "+path+" as '"+name+"'")

//    analysis.Util.assertProperCallStructure(new TraceFileIterator(tracefile))

    val db = new SpencerDB(name)
    db.loadFrom(path)
    println("done")
    sys.exit(0)
  }
}

package com.github.kaeluka.spencer

import java.io._
import java.net.URLClassLoader
import java.nio.file.{Path, Paths}
import java.util.function.Consumer
import java.util.zip.ZipEntry

import org.apache.commons.compress.archivers.zip.{ZipArchiveEntry, ZipFile}
import com.github.kaeluka.spencer.tracefiles.SpencerDB
import org.apache.commons.io.{FileUtils, IOUtils}

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

    val oBytecodeDir = args
      .find(_.startsWith("bytecode-dir="))
      .map(_.replace("bytecode-dir=", ""))

    if (! oBytecodeDir.isDefined) {
      System.err.println("No bytecode directory given. Use the 'bytecode-dir=...' switch.")
      sys.exit(1)
    }

    val rest = args.filter(!_.contains("="))

//    println((SpencerLoad.getClass().getClassLoader()).asInstanceOf[URLClassLoader].getURLs.mkString(",\n"))

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
    db.loadFrom(path, Paths.get(oBytecodeDir.get))
    println("done")
    sys.exit(0)
  }
}

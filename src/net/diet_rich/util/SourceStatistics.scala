package net.diet_rich.util

import org.apache.commons.io.FileUtils
import java.io.File
import scala.collection.JavaConverters._
import scala.io.Source

object SourceStatistics extends App {

  // restrict to .scala extension
  val scalaFiles = FileUtils iterateFiles (new File("src"), Array("scala"), true) asScala

  val (nloc, ncss) = scalaFiles.foldLeft((0,0)){case ((nlocSum,ncssSum), file) =>
    val filePathToPrint = file getPath() substring(4) replaceAll("\\\\","/")
    val lines = Source.fromFile(file, "UTF-8").getLines
    
    var inComment = false
    val sourceLines = lines
    // remove "//" comments
    .map(line => if (line contains "//") line.substring(0, line.indexOf("//")) else line)
    // remove "/* */" comments
    .map{line =>
      if (inComment) {
        if (line contains "*/") {
          inComment = false
          line.substring(line.indexOf("*/") + 2)
        } else ""
      } else {
        if (line contains "/*") {
          if (line contains "*/") {
            line.substring(0, line.indexOf("/*")) + line.substring(line.indexOf("*/") + 2)
          } else {
            inComment = true
            line.substring(0, line.indexOf("/*"))
          }
        } else line
      }
    }
    // trim leading and trailing blanks and filter empty lines
    .map(_ trim)
    .filterNot(_.isEmpty)
    .toList

    val statements = sourceLines flatMap (_.split("[\\s^!\"%&/(){}=+*~#<>|;,:-]")) filterNot (_ isEmpty)
    
    val nloc = sourceLines.size
    val ncss = statements.size
    println("%5d nloc %6d ncss in %s" format (nloc, ncss, filePathToPrint))
    
    (nlocSum + nloc, ncssSum + ncss)
  }

  println("%5d nloc %6d ncss total. (%1$d/%2$d)" format (nloc, ncss))
  
}
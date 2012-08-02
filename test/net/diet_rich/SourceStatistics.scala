// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich

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

    def removeFullComments(line: String) : (String, Boolean) = {
      if (line contains "/*") {
        if (line contains "*/") {
          removeFullComments(
            line.substring(0, line.indexOf("/*")) + line.substring(line.indexOf("*/") + 2)
          )
        } else {
          (line.substring(0, line.indexOf("/*")), true)
        }
      } else (line, false)
    }
    
    // remove "/* */" comments
    val (noCommentLines, endsInComment) = lines.foldLeft((List[String](), false)) {
      case ((result, false), line) =>
        val (cutLine, inComment) = removeFullComments(line)
        (cutLine :: result, inComment)
        
      case ((result, true), line) =>
        if (line contains "*/") {
          val (cutLine, inComment) =
            removeFullComments(line.substring(line.indexOf("*/") + 2))
          (cutLine :: result, inComment)
        } else (result, true)
    }

    // remove "//" comments
    val preparedLines = noCommentLines
    .map(line => if (line contains "//") line.substring(0, line.indexOf("//")) else line)

    val sourceLines = preparedLines
    .map(_ trim)
    .filterNot(_.isEmpty)

    val statements = sourceLines flatMap (_.split("[\\s^!\"%&/(){}=+*~#<>|;,:-]")) filterNot (_ isEmpty)

    val nloc = sourceLines.size
    val ncss = statements.size
    println("%5d nloc %6d ncss in %s" format (nloc, ncss, filePathToPrint))

    (nlocSum + nloc, ncssSum + ncss)
  }

  println("%5d nloc %6d ncss total. (%1$d/%2$d)" format (nloc, ncss))
  
}
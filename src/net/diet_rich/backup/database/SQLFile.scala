// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.database

object SQLFile {

  private val sqlFileStream = getClass getResourceAsStream "database.sql"
  val lines = (io.Source fromInputStream sqlFileStream getLines) toList
  
  val map = lines.foldLeft((Map[String, List[String]](), ""))((state, line) => {
    val (map, key) = state
    if (line.matches("\\[.*\\].*")) {
      val endIndex = line.lastIndexOf("]")
      val newKey = line.substring(1, endIndex)
      val restOfLine = line.substring(endIndex + 1)
      (map + ((newKey, restOfLine :: map.get(newKey).getOrElse(Nil))), newKey)
    } else {
      (map + ((key, line :: map.get(key).getOrElse(Nil))), key)
    }
  })._1.mapElements(_ reverse)
  
  def section(name: String) = {
    lines.partition(_ startsWith ("["+name+"]"))._2
  }
  
}
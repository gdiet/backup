// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.io

object SectionDataFile {

  def readSections(input: java.io.InputStream) : Map[String, List[String]] = 
    getSections(io.Source.fromInputStream(input).getLines)

  def getSections(lines: Iterator[String]) : Map[String, List[String]] =
    lines.foldLeft((Map[String, List[String]](), ""))((mapAndKey, line) => {
      val (map, currentKey) = mapAndKey
      if (line.matches("\\s*\\[.*\\].*")) {
        val endIndex = line.indexOf("]")
        val newKey = line.substring(line.indexOf("[") + 1, endIndex)
        (map + ((newKey, map.get(newKey).getOrElse(Nil))), newKey)
      } else {
        (map + ((currentKey, line :: map.get(currentKey).getOrElse(Nil))), currentKey)
      }
    })
    ._1 // get map, discard info about last key found
    .mapValues(_ reverse) // reverse all lists (they were built in reverse order)
    .toMap // make an immutable map

  def removeComments(lines: List[String]) : List[String] =
    lines.flatMap(_.split("//") match {
      case Array() => None
      case array: Array[String] => Some(array(0).reverse.dropWhile(_ == ' ').reverse)
    })

  def insertVariables(lines: List[String], vars: Map[String, String]) : List[String] = {
    @annotation.tailrec
    def replaceVarsIn(line : String) : String = {
      val start = line.indexOf("${")
      if (start == -1) line else {
        val end = line.indexOf("}", start)
        val newline = 
          line.substring(0, start) +
          vars.apply(line.substring(start+2, end)) +
          line.substring(end+1)
        replaceVarsIn(newline)
      }
    }
    lines.map( line => { replaceVarsIn(line) } )
  }
  
}
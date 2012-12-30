// Copyright (c) 2013 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.repository

import net.diet_rich.util._

object Create extends App {
  val defaults = Map(
    "-d" -> ".",
    "-h" -> "MD5"
  )

  val usage: String = try { """
    Creates a dedup repository. Parameters:
    [-d <directory>] Location of the repository to create, default "%s"
    [-h <algorithm>] Hash algorithm to use, default "%s"
    """.format(defaults("-d"), defaults("-h")).normalizeMultiline
  } catch { case e: Throwable => "Oops ... error while building usage string!" }
  
  try {
    val opts = defaults ++ Args.toMap(args)
    require(opts.keySet == defaults.keySet, "Unexpected parameter(s): %s" format (opts.keySet -- defaults.keySet).mkString(" / "))
  } catch {
    case e: Throwable =>
      println("%s\n\n%s\n%s" format (usage, e, e.getStackTraceString))
  }
  
}

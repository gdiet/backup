// Copyright (c) 2013 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.repository

import net.diet_rich.util.CmdApp

object Create extends CmdApp {
  val usageHeader = "Creates a dedup repository. "
  val paramData = Seq(
    "-d" -> "."   -> "[%s <directory>] Location of the repository to create, default '%s'",
    "-h" -> "MD5" -> "[%s <algorithm>] Hash algorithm to use, default '%s'"
  )
  
  main(args){ opts =>
    println("Create repository: Not yet implemented.")
  }
}

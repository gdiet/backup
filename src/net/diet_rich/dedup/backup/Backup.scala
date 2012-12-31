// Copyright (c) 2013 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.backup

import net.diet_rich.util.CmdApp

object Backup extends CmdApp {
  val usageHeader = "Stores a file or folder in the dedup repository. "
  val paramData = Seq(
    "-s" -> "." -> "[%s <directory>] Source file or folder to store, default '%s'",
    "-r" -> ""  -> "[%s <directory>] Mandatory: Repository location",
    "-t" -> ""  -> "[%s <path>] Mandatory: Target folder in repository"
  )

  main(args){ opts =>
    require(! opts("-r").isEmpty, "Repository location setting -r is mandatory.")
    require(! opts("-t").isEmpty, "Target folder setting -t is mandatory.")
    println("Store file or folder: Not yet implemented.")
  }
}
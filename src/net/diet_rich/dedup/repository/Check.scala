// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.repository

import java.io.File
import net.diet_rich.dedup.CmdLine._
import net.diet_rich.util._
import net.diet_rich.util.io._
import net.diet_rich.dedup.database._

object Check extends CmdApp {
  def main(args: Array[String]): Unit = run(args)
  
  protected val usageHeader = "Checks a dedup repository."
  protected val keysAndHints = Seq(
    REPOSITORY -> "" -> "[%s <directory>] Location of the repository to check",
    OPERATION -> "help" -> "[%s <operation>] Check to execute or 'help' to list available checks, default '%s'"
  )
  
  protected def application(con: Console, opts: Map[String, String]): Unit = {
    opts(OPERATION) match {
      case "help" =>
        con.println("Available checks:")
      case op =>
        con.println(s"'$op' is not a supported check.")
    }
  }
}

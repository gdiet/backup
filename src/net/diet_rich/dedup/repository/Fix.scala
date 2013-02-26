// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.repository

import java.io.File
import net.diet_rich.dedup.CmdLine._
import net.diet_rich.util._
import net.diet_rich.util.io._
import net.diet_rich.dedup.database._

object Fix extends CmdApp {
  def main(args: Array[String]): Unit = run(args)
  
  protected val usageHeader = "Repairs a dedup repository."
  protected val keysAndHints = Seq(
    REPOSITORY -> "" -> "[%s <directory>] Location of the repository to repair",
    OPERATION -> "help" -> "[%s <operation>] Repair operation to execute or 'help' to list available repairs, default '%s'"
  )
  
  protected val dataFileHeadersOp = "recreateDataFileHeaders"
   
  protected def application(con: Console, opts: Map[String, String]): Unit = {
    opts(OPERATION) match {
      
      case "help" =>
        con.println("Available repairs:")
        con.println(s"$dataFileHeadersOp - recreate the headers of all data files")
        
      case `dataFileHeadersOp` =>
        val repository = new Repository(new java.io.File(opts(REPOSITORY)), false)
        try {
          val ds = repository.dataStore
          val dataFileNumber = 0L
          ds.recreateDataFileHeader(dataFileNumber)
          @annotation.tailrec
          def recreate(dataFileNumber: Long): Long =
            if (ds.recreateDataFileHeader(dataFileNumber)) {
              con.printProgress(s"Processing data file $dataFileNumber")
              recreate(dataFileNumber + 1)
            } else dataFileNumber
          val dataFilesProcessed = recreate(0)
          con.println(s"Recreated headers of $dataFilesProcessed data files.")
          con.println("Cleaning up ...")
        } finally { repository.shutdown(false) }
        con.println("Finished.")
        
      case op =>
        con.println(s"'$op' is not a supported repair operation.")
    }
  }
}

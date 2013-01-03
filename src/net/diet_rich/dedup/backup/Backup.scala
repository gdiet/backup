// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.backup

import net.diet_rich.dedup.CmdLine._
import net.diet_rich.dedup.database._
import net.diet_rich.dedup.repository.Repository
import net.diet_rich.util.CmdApp

object Backup extends CmdApp {
  def main(args: Array[String]): Unit = run(args)(backup)
  
  val usageHeader = "Stores a file or folder in the dedup repository. "
  val paramData = Seq(
    SOURCE -> "." -> "[%s <directory>] Source file or folder to store, default '%s'",
    REPOSITORY -> "" -> "[%s <directory>] Mandatory: Repository location",
    TARGET -> "" -> "[%s <path>] Mandatory: Target folder in repository"
  )

  def backup(opts: Map[String, String]): Unit = {
    require(! opts("-r").isEmpty, "Repository location setting -r is mandatory.")
    require(! opts("-t").isEmpty, "Target folder setting -t is mandatory.")
    val repository = new Repository(new java.io.File(opts("-r")))
    val source = new java.io.File(opts("-s")).getCanonicalFile
    
    val processor =
      new TreeHandling[FileSource] 
      with SimpleBackupControl
      with SimpleMemoryManager
      with PrintMatchCheck[FileSource]
      with StoreData[FileSource] {
        val fs = repository.fs
      }
    
    processor.backup(new FileSource(source), TreeEntryID(0), None) // FIXME use parent appropriately
  }
}

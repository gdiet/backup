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
    TARGET -> "" -> "[%s <path>] Mandatory: Target folder in repository",
    DIFFERENTIAL -> "" -> "[%s <path>] Base folder for differential backup in repository"
  )

  def backup(opts: Map[String, String]): Unit = {
    require(! opts(REPOSITORY).isEmpty, "Repository location setting %s is mandatory." format REPOSITORY)
    require(! opts(TARGET).isEmpty, "Target folder setting %s is mandatory." format TARGET)
    val repository = new Repository(new java.io.File(opts(REPOSITORY)))
    val source = new java.io.File(opts(SOURCE)).getCanonicalFile
    val reference = opts(DIFFERENTIAL) match {
      case "" => None
      case e => repository.fs.entry(Path(e)) match {
        case None => throw new IllegalArgumentException("No path '%s' in repository for differential backup")
        case id => id
      }
    }
    val target = repository.fs.getOrMake(Path(opts(TARGET)))
    
    val processor =
      new TreeHandling[FileSource] 
//      with SimpleBackupControl
      with PooledBackupControl
      with SimpleMemoryManager
      with PrintMatchCheck[FileSource]
      with StoreData[FileSource] {
        val fs = repository.fs
      }

    try {
      processor.backup(new FileSource(source), target, reference)
    } finally {
      repository.dataStore.shutdown
      processor.shutdown
    }
  }
}

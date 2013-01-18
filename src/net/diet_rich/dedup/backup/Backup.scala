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
    require(! opts(REPOSITORY).isEmpty, s"Repository location setting $REPOSITORY is mandatory.")
    require(! opts(TARGET).isEmpty, s"Target folder setting $TARGET is mandatory.")
    val repository = new Repository(new java.io.File(opts(REPOSITORY)))
    val source = new java.io.File(opts(SOURCE)).getCanonicalFile
    val reference = opts(DIFFERENTIAL) match {
      case "" => None
      case e => repository.fs.entry(Path(e)) match {
        case None => throw new IllegalArgumentException("No path ${opts(DIFFERENTIAL)} in repository for differential backup")
        case id => id
      }
    }
    val target = repository.fs.getOrMake(Path(opts(TARGET)))
    if (!repository.fs.children(target).isEmpty)
      throw new IllegalArgumentException("Target folder ${opts(TARGET)} is not empty")
    if (!repository.fs.fullDataInformation(target).isEmpty)
      throw new IllegalArgumentException("Target ${opts(TARGET)} is a file, not a folder")
    
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
      processor.shutdown
      repository.dataStore.shutdown
    }
  }
}

// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.backup

import net.diet_rich.dedup.CmdLine._
import net.diet_rich.dedup.database._
import net.diet_rich.dedup.repository.Repository
import net.diet_rich.util.CmdApp

case class BackupSettings(storeMethod: Method)

object Backup extends CmdApp {
  def main(args: Array[String]): Unit = run(args)(backup)
  
  val usageHeader = "Stores a file or folder in the dedup repository. "
  val paramData = Seq(
    SOURCE -> "" -> "[%s <directory>] Mandatory: Source file or folder to store",
    REPOSITORY -> "" -> "[%s <directory>] Mandatory: Repository location",
    TARGET -> "" -> "[%s <path>] Mandatory: Target folder in repository (must be empty if exists)",
    DIFFERENTIAL -> "" -> "[%s <path>] Base folder for differential backup in repository",
    METHOD -> "1" -> "[%s Integer] Store method: 0 - store; 1 - deflate, default '%s'"
  )

  def backup(opts: Map[String, String]): Unit = {
    require(! opts(REPOSITORY).isEmpty, s"Repository location setting $REPOSITORY is mandatory.")
    require(! opts(TARGET).isEmpty, s"Target folder setting $TARGET is mandatory.")
    val storeMethod = Method(opts(METHOD).toInt)
    val repository = new Repository(new java.io.File(opts(REPOSITORY)))
    val source = new java.io.File(opts(SOURCE)).getCanonicalFile
    val reference = opts(DIFFERENTIAL) match {
      case "" => None
      case e => repository.fs.entry(Path(e)) match {
        case None => throw new IllegalArgumentException(s"No path ${opts(DIFFERENTIAL)} in repository for differential backup")
        case id => id
      }
    }
    val target = repository.fs.getOrMakeDir(Path(opts(TARGET)))
    if (!repository.fs.children(target).isEmpty)
      throw new IllegalArgumentException(s"Target folder ${opts(TARGET)} is not empty")
    if (!repository.fs.fullDataInformation(target).isEmpty)
      throw new IllegalArgumentException(s"Target ${opts(TARGET)} is a file, not a folder")
    
    val processor =
      new TreeHandling
//      with NoPrintMatchCheck[FileSource]
      with PrintMatchCheck
//      with IgnorePrintMatch[FileSource]
      with StoreData
      with AlgorithmCommons {
        type SourceType = FileSource
        protected val fs = repository.fs
//        protected val control = new SimpleBackupControl with SimpleMemoryManager
        protected val control = new PooledBackupControl with SimpleMemoryManager
        protected val settings = new BackupSettings(storeMethod)
      }

    val time = System.currentTimeMillis()
    try {
      println("starting backup")
      try { processor.backup(new FileSource(source), target, reference) }
      finally { processor.shutdown }
      println(s"finished backup, cleaning up. Time: ${(System.currentTimeMillis() - time)/1000d}")
    } finally {
      repository.shutdown(true)
      println(s"shutdown complete. Time: ${(System.currentTimeMillis() - time)/1000d}")
    }
  }
}

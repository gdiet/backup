// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.backup

import net.diet_rich.dedup.CmdLine._
import net.diet_rich.dedup.database._
import net.diet_rich.dedup.repository.Repository
import net.diet_rich.util.CmdApp
import net.diet_rich.util.Strings

case class BackupSettings(storeMethod: Method)

object Backup extends CmdApp {
  def main(args: Array[String]): Unit = run(args)
  
  protected val usageHeader = "Stores a file or folder in the dedup repository. "
  protected val paramData = Seq(
    SOURCE -> "" -> "[%s <directory>] Mandatory: Source file or folder to store",
    REPOSITORY -> "" -> "[%s <directory>] Mandatory: Repository location",
    TARGET -> "" -> "[%s <path>] Mandatory: Target folder in repository (must be empty if exists)",
    DIFFERENTIAL -> "" -> "[%s <path>] Base folder for differential backup in repository",
    METHOD -> "1" -> "[%s Integer] Store method: 0 - store; 1 - deflate, default '%s'"
  )

  protected def application(opts: Map[String, String]): Unit = {
    require(! opts(REPOSITORY).isEmpty, s"Repository location setting $REPOSITORY is mandatory.")
    require(! opts(TARGET).isEmpty, s"Target folder setting $TARGET is mandatory.")
    require(! opts(SOURCE).isEmpty, s"Source setting $SOURCE is mandatory.")
    val storeMethod = Method(opts(METHOD).toInt)
    val source = new java.io.File(opts(SOURCE)).getCanonicalFile
    require(source.exists(), s"Source $source does not exist.")
    val targetString = opts(TARGET)
    require(targetString.count('|'==) % 2 == 0, "Target string is not well-formed with respect to '|'")
    
    val date = new java.util.Date
    val dateTargetPath = Path(
      Strings.processSpecialSyntax(targetString, identity, new java.text.SimpleDateFormat(_).format(date))
    )
    if (targetString.contains('|')) println(s"Storing in target $dateTargetPath")
    
    val repository = new Repository(new java.io.File(opts(REPOSITORY)))
      
    val reference = opts(DIFFERENTIAL) match {
      case "" => None
      case e => repository.fs.entryWithWildcards(Path(e)) match {
        case None => throw new IllegalArgumentException(s"No path ${opts(DIFFERENTIAL)} in repository for differential backup")
        case id =>
          println("Backup differential to " + repository.fs.path(id.get.id))
          id
      }
    }
    if (!repository.fs.entry(dateTargetPath).isEmpty)
      throw new IllegalArgumentException(s"Target $dateTargetPath already exists")
    val parent = repository.fs.getOrMakeDir(dateTargetPath.parent)
    if (!repository.fs.fullDataInformation(parent).isEmpty)
      throw new IllegalArgumentException(s"Target's parent ${dateTargetPath.parent} is a file, not a folder")
    
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
      try { processor.backup(dateTargetPath.name, new FileSource(source), parent, reference) }
      finally { processor.shutdown }
      println(s"finished backup, cleaning up. Time: ${(System.currentTimeMillis() - time)/1000d}")
    } finally {
      repository.shutdown(true)
      println(s"shutdown complete. Time: ${(System.currentTimeMillis() - time)/1000d}")
    }
  }
}

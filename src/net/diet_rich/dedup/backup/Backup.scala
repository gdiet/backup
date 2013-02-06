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
  
  protected val usageHeader = "Stores a file or folder in the dedup repository."
  protected val keysAndHints = Seq(
    SOURCE -> "" -> "[%s <directory>] Source file or folder to store",
    REPOSITORY -> "" -> "[%s <directory>] Repository location",
    TARGET -> "" -> "[%s <path>] Target folder in repository (must be empty if exists)",
    METHOD -> "1" -> "[%s Integer] Store method: 0 - store; 1 - deflate, default '%s'"
  )
  override protected val optionalKeysAndHints = Seq(
    DIFFERENTIAL -> "" -> "[%s <path>] Base folder for differential backup in repository",
    INTERACTIVE -> "y" -> "[%s [y|n]] If not 'y', no user interaction needed, default '%s'"
  )

  protected def application(opts: Map[String, String]): Unit = {
    val storeMethod = Method(opts(METHOD).toInt)
    val source = new java.io.File(opts(SOURCE)).getCanonicalFile
    require(source.canRead(), s"Can't read source $source")
    
    val dateTargetPath = Path(
      Strings.processSpecialSyntax(opts(TARGET), identity, new java.text.SimpleDateFormat(_).format(new java.util.Date))
    )
    
    val repository = new Repository(new java.io.File(opts(REPOSITORY)))
      
    val reference = opts(DIFFERENTIAL) match {
      case "" => None
      case e => repository.fs.entryWithWildcards(Path(e)) match {
        case None => throw new IllegalArgumentException(s"No path ${opts(DIFFERENTIAL)} in repository for differential backup")
        case id => id
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

    println("Backup settings:")
    println(s"Source: $source")
    println(s"Target: $dateTargetPath")
    println(s"Method: $storeMethod")
    reference match {
      case Some(reference) => println("DiffTo: " + repository.fs.path(reference.id))
      case None => println("NoDiff: ----")
    }
    
    if (opts(INTERACTIVE) != "y" || Console.readLine("\nStart backup? [y/n] ") == "y") {
      val time = System.currentTimeMillis()
      try {
        println("starting backup")
        try {
          // the shutdown hook is for catching CTRL-C
          val shutdownHook = sys.ShutdownHookThread {
            println("Backup interrupted, shutting down...")
            processor.shutdown
            repository.shutdown(true)
            println("Shutdown complete.")
          }
          processor.backup(dateTargetPath.name, new FileSource(source), parent, reference)
          shutdownHook.remove
      	} finally { processor.shutdown }
        println(s"finished backup, cleaning up. Time: ${(System.currentTimeMillis() - time)/1000d}")
      } finally {
        repository.shutdown(true)
        println(s"shutdown complete. Time: ${(System.currentTimeMillis() - time)/1000d}")
      }
    }
  }
}

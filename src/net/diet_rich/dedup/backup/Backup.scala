// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.backup

import net.diet_rich.dedup.CmdLine._
import net.diet_rich.dedup.database._
import net.diet_rich.dedup.plugins.ConsoleProgressOutput
import net.diet_rich.dedup.repository.Repository
import net.diet_rich.util._

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

  protected def application(con: Console, opts: Map[String, String]): Unit = {
    val storeMethod = Method(opts(METHOD).toInt)
    val source = new java.io.File(opts(SOURCE)).getCanonicalFile
    require(source.canRead(), s"Can't read source $source")
    val fileSource = new FileSource(source)
    
    val dateTargetPath = Path(
      Strings.processSpecialSyntax(opts(TARGET), identity, new java.text.SimpleDateFormat(_).format(new java.util.Date))
    )
    
    val repository = new Repository(new java.io.File(opts(REPOSITORY)), false)
    import repository.fs
    
    val (reference, referenceMessage) = opts(DIFFERENTIAL) match {
      case "" => (None, "")
      case e => fs.entryWithWildcards(Path(e)) match {
        case None => throw new IllegalArgumentException(s"No path ${opts(DIFFERENTIAL)} in repository for differential backup")
        case Some(entry) =>
          def matchSourceAndDiff(source: FileSource, diff: TreeEntry): Map[FileSource, TreeEntry] = {
            val diffs = fs.children(diff.id).toList
            source.children.flatMap { file =>
              diffs.find(_.name == file.name).map(entry => (file -> entry))
            }.toMap
          }
          val matching1 = matchSourceAndDiff(fileSource, entry)
          val matching2 = matching1.flatMap { case (file, entry) =>
            matchSourceAndDiff(file, entry)
          }
          require(!matching1.isEmpty, s"No matches in differential path ${fs.path(entry.id).getOrElse("- ERROR -")}")
          val message = s"Differential matches for level 1: ${matching1.size}\n" +
            s"Differential matches for level 2: ${matching2.size}"
          (Some(entry), message)
      }
    }
    if (!repository.fs.entry(dateTargetPath).isEmpty)
      throw new IllegalArgumentException(s"Target $dateTargetPath already exists")
    val parent = repository.fs.getOrMakeDir(dateTargetPath.parent)
    if (!repository.fs.fullDataInformation(parent).isEmpty)
      throw new IllegalArgumentException(s"Target's parent ${dateTargetPath.parent} is a file, not a folder")
    
    val storeAlgorithm = new StoreAlgorithm[FileSource] (
      repository.fs,
      SimpleMemoryManager,
      storeMethod,
      new PrintMatchCheck(repository.digesters.calculatePrint _)
    )
    
    val progressOutput = new ConsoleProgressOutput(con, "backup: %s files in %s directories after %ss")
    val control = new PooledBackupControl(con, progressOutput)
    
    val processor = new BackupProcessor[FileSource] (
      control,
      repository.fs,
      storeAlgorithm
    )

    con.println("Backup settings:")
    con.println(s"Source: $source")
    con.println(s"Target: $dateTargetPath")
    con.println(s"Method: $storeMethod")
    reference match {
      case Some(reference) =>
        con.println("DiffTo: " + repository.fs.path(reference.id).getOrElse("- ERROR -"))
        con.println(referenceMessage)
      case None => con.println("NoDiff: ----")
    }
    
    if (opts(INTERACTIVE) != "y" || con.readln("\nStart backup? [y/n] ") == "y") {
      val time = System.currentTimeMillis()
      try {
        con.println("starting backup")
        progressOutput.start
        try {
          // the shutdown hook is for catching CTRL-C
          val shutdownHook = sys.ShutdownHookThread {
            con.println("Backup interrupted, shutting down...")
            control.shutdown
            repository.shutdown(true)
            con.println("Shutdown complete.")
          }
          processor.backup(dateTargetPath.name, fileSource, parent, reference)
          shutdownHook.remove
      	} finally { control.shutdown }
        con.println(s"finished backup, cleaning up. Time: ${(System.currentTimeMillis() - time)/1000d}")
      } finally {
        repository.shutdown(true)
        con.println(s"Shutdown complete. Time: ${(System.currentTimeMillis() - time)/1000d}")
      }
    } else {
      control.shutdown
      repository.shutdown(false)
    }
  }
}

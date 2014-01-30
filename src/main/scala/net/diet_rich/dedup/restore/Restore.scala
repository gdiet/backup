// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.restore

import net.diet_rich.dedup.CmdLine._
import net.diet_rich.dedup.database._
import net.diet_rich.dedup.plugins.ConsoleProgressOutput
import net.diet_rich.dedup.repository.Repository
import net.diet_rich.util._
import net.diet_rich.util.io._
import java.io.File
import java.io.RandomAccessFile

object Restore extends CmdApp {
  def main(args: Array[String]): Unit = run(args)
  
  val usageHeader = "Restores a file or folder from the dedup repository."
  val keysAndHints = Seq(
    SOURCE -> "" -> "[%s <path>] Source file or folder to restore",
    REPOSITORY -> "" -> "[%s <directory>] Repository location",
    TARGET -> "" -> "[%s <directory>] Target directory to restore to (must be empty)"
  )

  protected def application(con: Console, opts: Map[String, String]): Unit = {
    val repository = new Repository(new java.io.File(opts(REPOSITORY)), true)
    val source = repository.fs.entryWithWildcards(Path(opts(SOURCE))) match {
      case None => throw new IllegalArgumentException("Source path ${opts(SOURCE)} not in repository")
      case Some(entry) => entry
    }
    if (source.dataid isDefined)
      throw new IllegalArgumentException("Source ${opts(SOURCE)} has data but must be a pure directory")
    val target = new File(opts(TARGET))
    require(!target.exists(), s"Target $target must not exist.")
    target.getParentFile().mkdirs()
    require(target.getParentFile().isDirectory(), "Can't create target's $target parent folder.")
    
    con.println(s"Restoring files:")
    con.println(s"Repository:  ${opts(REPOSITORY)}")
    con.println(s"Source path: ${opts(SOURCE)}")
    con.println(s"Target path: ${opts(TARGET)}")
    
    using(new ConsoleProgressOutput(con, "restore: %s files in %s directories after %ss")) { progressOutput =>
      progressOutput.start
      try { doRestore(con, repository, source, target.getName(), target.getParentFile(), progressOutput) }
      finally { repository.shutdown(false) }
    }
  }
  
  private def doRestore(con: Console, repository: Repository, source: TreeEntry, name: String, targetParent: File, progressOutput: ConsoleProgressOutput): Unit = {
    val target = targetParent.child(name)
    val children = repository.fs.children(source.id).toList
    source.dataid match {
      case None =>
        progressOutput.incDirs
        require(target.mkdir(), s"Can't create $target")
        children.foreach(child => doRestore(con, repository, child, child.name, target, progressOutput))
      case Some(dataid) =>
        progressOutput.incFiles
        val dataEntry = repository.fs.dataEntry(dataid)
        require(children.isEmpty, "Expected no children for node $source with data")
        val (print, (hash, size)) = using(new RandomAccessFile(target, "rw")) { sink =>
          val source = repository.fs.read(dataid, dataEntry.method)
          repository.digesters.filterPrint(source)(source =>
            repository.digesters.filterHash(source)(source =>
              source.copyTo(sink)
            )
          )
        }
        if (size != dataEntry.size.value) System.err.println(s"ERROR: Data size $size did not match ${dataEntry.size} for $target")
        if (print != dataEntry.print) System.err.println(s"ERROR: Data print $print did not match ${dataEntry.print} for $target")
        if (hash !== dataEntry.hash) System.err.println(s"ERROR: Data hash did not match for $target")
        try { target.setLastModified(source.time.value) }
        catch { case e: Throwable =>
          // workaroud for java bug: can read, but can't set negative time stamp
          // TODO 09 collect problem
          con.println(s"WARNING: Could not set time ${source.time} for $target - $e")
        }
    }
  }
}

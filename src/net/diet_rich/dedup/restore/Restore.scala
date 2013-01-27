// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.restore

import net.diet_rich.dedup.CmdLine._
import net.diet_rich.dedup.database._
import net.diet_rich.dedup.repository.Repository
import net.diet_rich.util.CmdApp
import net.diet_rich.util.io._
import java.io.File
import java.io.RandomAccessFile

object Restore extends CmdApp {
  def main(args: Array[String]): Unit = run(args)(restore)
  
  val usageHeader = "Restores a file or folder from the dedup repository. "
  val paramData = Seq(
    SOURCE -> "" -> "[%s <path>] Source file or folder to restore",
    REPOSITORY -> "" -> "[%s <directory>] Mandatory: Repository location",
    TARGET -> "" -> "[%s <directory>] Mandatory: Target directory to restore to (must be empty)"
  )

  def restore(opts: Map[String, String]): Unit = {
    require(! opts(REPOSITORY).isEmpty, s"Repository location setting $REPOSITORY is mandatory.")
    require(! opts(SOURCE).isEmpty, s"Source path setting $SOURCE is mandatory.")
    require(! opts(TARGET).isEmpty, s"Target folder setting $TARGET is mandatory.")
    val repository = new Repository(new java.io.File(opts(REPOSITORY)))
    val source = repository.fs.entryWithWildcards(Path(opts(SOURCE))) match {
      case None => throw new IllegalArgumentException("Source path ${opts(SOURCE)} not in repository")
      case Some(id) => id
    }
    val target = new File(opts(TARGET))
    if (target.exists()) {
      require(target.isDirectory(), "Target must be a directory.")
      require(target.list().isEmpty, "Target directory must be empty.")
    } else {
      require(target.mkdirs(), "Can't create target folder.")
    }
    
    restore(repository, source, target)
  }
  
  private def restore(repository: Repository, source: TreeEntry, targetParent: File): Unit = {
    val target = targetParent.child(source.name)
    val children = repository.fs.children(source.id).toList
    source.dataid match {
      case None =>
        require(target.mkdir(), f"Can't create ${targetParent.child(source.name)}")
        children.foreach(restore(repository, _, target))
      case Some(dataid) =>
        val dataEntry = repository.fs.dataEntry(dataid)
        require(children.isEmpty, f"Expected no children for node $source with data")
        val (print, (hash, size)) = using(new RandomAccessFile(target, "rw")) { sink =>
          val source = repository.fs.read(dataid, dataEntry.method)
          repository.digesters.filterPrint(source)(source =>
            repository.digesters.filterHash(source)(source =>
              source.copyTo(sink)
            )
          )
        }
        if (size != dataEntry.size.value) System.err.println(f"ERROR: Data size $size did not match ${dataEntry.size} for $target")
        if (print != dataEntry.print) System.err.println(f"ERROR: Data print $print did not match ${dataEntry.print} for $target")
        if (hash !== dataEntry.hash) System.err.println(f"ERROR: Data hash did not match for $target")
        target.setLastModified(source.time.value)
    }
  }
}

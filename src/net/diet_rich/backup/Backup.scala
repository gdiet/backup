// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup

import java.io.File
import net.diet_rich.util.Executor
import net.diet_rich.backup.db.TreeDB
import net.diet_rich.backup.db.TreeEntry

object BackupApp extends App {
  if (args.length < 3) throw new IllegalArgumentException("Backup needs at least source, repository and target arguments.")
  if (args.length > 4) throw new IllegalArgumentException("Too many arguments.")

  val source = new File(args(0))
  val repository = args(1)
  val targetPath = args(2)
  val referencePath = if (args.length < 3) None else Some(args(3))
  
  if (!source.canRead) throw new IllegalArgumentException("Cannot read source.")
  if (!targetPath.startsWith("/")) throw new IllegalArgumentException("Target path must start with '/'.")
  
  val connection = Repository connectToDB new File(repository)
  val hashAlgorithm = Repository readHashAlgorithm connection
  val dbExecutor = Executor(1, 100) // currently, 1 thread is best for h2 file db - FIXME configuration
  val tree = TreeDB.deferredInsertDB(connection, dbExecutor)
  val processExecutor = Executor(8, 1) // FIXME configuration

  val target = tree.getOrMake(targetPath)
  if (! tree.children(target).isEmpty) throw new IllegalAccessException("Target node is not empty.")
  
  val reference = referencePath.flatMap(tree entry _)
  if (referencePath.isDefined && reference.isEmpty) throw new IllegalArgumentException("Reference not found in repository.")
  
  val backup = new BackupElements(tree)
  new Backup(backup, hashAlgorithm, processExecutor).process(source, reference, target)
}

class BackupElements(val tree: TreeDB)

class Backup(backup: BackupElements, hashAlgorithm: String, executor: Executor) {
  val hashInputMarkSupported = HashCalcInput.markSupported(hashAlgorithm)
  
  def process(source: File, reference: Option[TreeEntry], parent: Long): Unit = executor {
    if (source.isFile)
      processFile(source, reference, parent)
    else {
      val name = source.getName
      val dir = backup.tree create(parent, name)
      val ref = reference flatMap(ref => backup.tree children(ref.id) find(_.name == name))
      source.listFiles foreach(process(_, ref, dir))
    }
  }
  
  private def processFile(source: File, reference: Option[TreeEntry], parent: Long): Unit = {
    throw new UnsupportedOperationException
  }
}

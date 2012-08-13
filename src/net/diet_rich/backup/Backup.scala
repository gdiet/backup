// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup

import java.io.File
import net.diet_rich.util.Executor
import net.diet_rich.backup.db._
import java.io.RandomAccessFile

object BackupApp extends App {
  if (args.length < 3) throw new IllegalArgumentException("Backup needs at least source, repository and target arguments.")
  if (args.length > 4) throw new IllegalArgumentException("Too many arguments.")

  val source = new File(args(0))
  val repository = args(1)
  val targetPath = args(2)
  val referencePath = if (args.length < 3) None else Some(args(3))
  
  if (!source.canRead) throw new IllegalArgumentException("Cannot read source.")
  if (!targetPath.startsWith("/")) throw new IllegalArgumentException("Target path must start with '/'.")
  
  implicit val connection = Repository connectToDB new File(repository)
  val hashAlgorithm = Repository readHashAlgorithm connection
  val dbExecutor = Executor(1, 100) // currently, 1 thread is best for h2 file db - FIXME configuration
  val tree = TreeDB deferredInsertDB dbExecutor
  val data = DataInfoDB deferredWriteDB dbExecutor
  val processExecutor = Executor(8, 1) // FIXME configuration

  val target = tree.getOrMake(targetPath)
  if (! tree.children(target).isEmpty) throw new IllegalAccessException("Target node is not empty.")
  
  val reference = referencePath.flatMap(tree entry _)
  if (referencePath.isDefined && reference.isEmpty) throw new IllegalArgumentException("Reference not found in repository.")

  val settings = new BackupSettings(true) // FIXME configuration
  val backup = new BackupElements(tree, data, CrcAdler8192, settings)
  new Backup(backup, hashAlgorithm, processExecutor).process(source, reference, target)
}

class BackupSettings(val printForMatch: Boolean)

class BackupElements(val tree: TreeDB, val data: DataInfoDB, val print: PrintDigester, val set: BackupSettings)

class Backup(backup: BackupElements, hashAlgorithm: String, executor: Executor) {
  def process(source: File, reference: Option[TreeEntry], parent: Long): Unit = executor {
    if (source.isFile) {
      if (reference.isDefined)
        processFileWithReference(source, reference get, parent)
      else
        processFile(source, parent)
    } else {
      val name = source.getName
      val dir = backup.tree create(parent, name)
      val ref = reference flatMap(ref => backup.tree children(ref.id) find(_.name == name))
      source.listFiles foreach(process(_, ref, dir))
    }
  }
  
  private def processFileWithReference(source: File, reference: TreeEntry, parent: Long): Unit = {
    val referenceData = backup.data.read(reference.id)
    if (source.lastModified != reference.time || source.length != referenceData.length)
      processFile(source, parent)
    else if (backup.set.printForMatch) {
      val input = new RandomAccessFile(source, "r")
      val printResult = backup.print.print(input)
      if (printResult != referenceData.print) {
        input.seek(0)
        throw new UnsupportedOperationException // FIXME
      } else
        backup.tree.create(parent, source.getName, reference.time, reference.dataid)
    } else
      backup.tree.create(parent, source.getName, reference.time, reference.dataid)
  }

  private def processFile(source: File, parent: Long): Unit = {
    throw new UnsupportedOperationException // FIXME
  }

  
  
}

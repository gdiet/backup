// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup

import java.io.File
import net.diet_rich.util.Executor
import net.diet_rich.util.io._
import net.diet_rich.backup.db._
import java.io.RandomAccessFile
import net.diet_rich.util.ImmutableBytes

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

  val settings = new BackupSettings(true, hashAlgorithm) // FIXME printForMatch -> configuration
  val backup = new BackupElements(tree, data, CrcAdler8192, settings) // FIXME: CrcAdler8192 -> configuration
  new Backup(backup, processExecutor).process(source, reference, target)
}

class BackupSettings(val printForMatch: Boolean, val hashAlgorithm: String)

class BackupElements(val tree: TreeDB, val data: DataInfoDB, val print: PrintDigester, val set: BackupSettings)

class Backup(backup: BackupElements, executor: Executor) { import Backup._
  HashCalcInput.digester(backup.set.hashAlgorithm).clone // making sure that the hash digester supports clone
  
  def process(source: File, reference: Option[TreeEntry], parent: Long): Unit = executor {
    if (source.isFile)
      processFileWithOrWithoutReference(source, reference, parent)
    else {
      val name = source.getName
      val dir = backup.tree create(parent, name)
      val ref = reference flatMap(ref => backup.tree children(ref.id) find(_.name == name))
      source.listFiles foreach(process(_, ref, dir))
    }
  }

  private def processFileWithOrWithoutReference(source: File, reference: Option[TreeEntry], parent: Long): Unit = {
    if (reference.isDefined)
      processFileWithReference(source, reference get, parent)
    else
      processFile(source, parent)
  }

  private def processFileWithReference(source: File, reference: TreeEntry, parent: Long): Unit = {
    val referenceData = backup.data.read(reference.id)
    if (source.lastModified != reference.time || source.length != referenceData.length)
      processFile(source, parent)
    else
      processFileWithMatchingTimeAndSize(source, reference, referenceData.print, parent)
  }

  private def processFileWithMatchingTimeAndSize(source: File, reference: TreeEntry, referencePrint: Long, parent: Long): Unit = {
    if (backup.set.printForMatch)
      using(new RandomAccessFile(source, "r"))(processPrintMatchForInput(source, _, reference, referencePrint, parent))
    else
      backup.tree.create(parent, source.getName, reference.time, reference.dataid)
  }
  
  private def processPrintMatchForInput(source: File, input: RandomAccessFile, reference: TreeEntry, referencePrint: Long, parent: Long): Unit = {
    if (backup.print.print(input).print != referencePrint) {
      input.seek(0)
      processInput(source, input, parent)
    } else
      backup.tree.create(parent, source.getName, reference.time, reference.dataid)
  }
  
  private def processFile(source: File, parent: Long): Unit =
    using(new RandomAccessFile(source, "r"))(processInput(source, _, parent))

  private def processInput(source: File, input: RandomAccessFile, parent: Long): Unit =
    processHashedInput(source, new HashCalcInput(input, backup.set.hashAlgorithm), parent)

  private def processHashedInput(source: File, input: HashCalcInput, parent: Long): Unit = {
    val print = backup.print.print(input)
    if (!print.data.isFull)
      processInMemoryInput(source, print.print, List(print.data), input, parent)
    else
      processHashedInputWithPrint(source, print, input, parent)
  }
    
  private def processHashedInputWithPrint(source: File, print: PrintResult, input: HashCalcInput, parent: Long): Unit = {
    val data = readAsMuchAsPossibleToMemory(input)
    if (!data.isFull)
      processInMemoryInput(source, print.print, List(print.data, data), input, parent)
    else
      processInputThatDoesNotFitIntoMemory(source, print.print, List(print.data, data), input, parent)
  }

  private def processInMemoryInput(source: File, print: Long, data: List[ImmutableBytes], input: HashCalcInput, parent: Long): Unit = {
    throw new UnsupportedOperationException // FIXME
  }

  private def processInputThatDoesNotFitIntoMemory(source: File, print: Long, data: List[ImmutableBytes], input: HashCalcInput, parent: Long): Unit = {
    throw new UnsupportedOperationException // FIXME
  }
  
}

object Backup {
  
  val MEMORYRESERVED = 1024*1024*64 // FIXME configuration
  val MAXMEMRATIO = 0.75 // FIXME configuration
  val MINMEMORYCHUNK = 1024 // FIXME configuration

  def freeMemory: Long = { val rt = Runtime.getRuntime; rt.maxMemory - (rt.totalMemory - rt.freeMemory) }

  def availableArraySize: Int = {
    val availableMemory = math.max(0, freeMemory - MEMORYRESERVED)
    math.min(Int.MaxValue, availableMemory * MAXMEMRATIO).toInt
  }
  
  /** @return A byte array, if possible of the size indicated, but at least
   *  of the size MINMEMORYCHUNK.
   */
  def applyForLargeByteArray(size: Long): Array[Byte] = {
    val sizeRestrictedToInt = math.min(Int.MaxValue, size).toInt
    val sizeWithoutMinimum = math.min(availableArraySize, sizeRestrictedToInt)
    val arraySize = math.max(MINMEMORYCHUNK, sizeWithoutMinimum)
    new Array[Byte](arraySize)
  }

  def readAsMuchAsPossibleToMemory(input: HashCalcInput): ImmutableBytes = {
    val data = applyForLargeByteArray(input.remaining + 1)
    ImmutableBytes(data, fillFrom(input, data, 0, data.length))
  }

}
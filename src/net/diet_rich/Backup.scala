package net.diet_rich

import java.io.File
import net.diet_rich.fdfs._
import net.diet_rich.util.closureToRunnable


object Backup extends App {
  if (args.length < 3) throw new IllegalArgumentException("Backup needs at least source, repository and target arguments")
  if (args.length > 3) throw new IllegalArgumentException("Too many arguments")
  val source = new File(args(0))
  val repository = new File(args(1))
  val target = args(2)
  new Backup(source, repository, target)

  val MEMORYRESERVED = 1024*1024*64
  val LARGEARRAYSIZE = 1024*1024*2

  // FIXME make a real memory management
  def loanLargeByteArraysIfPossible(size: Long): List[Array[Byte]] = {
    val runtime = Runtime.getRuntime
    val maxFree = runtime.maxMemory - (runtime.totalMemory - runtime.freeMemory)
    if (size > maxFree + MEMORYRESERVED) Nil else {
      val blocks = (size - 1) / LARGEARRAYSIZE + 1
      (for (i <- 1L to blocks) yield new Array[Byte](LARGEARRAYSIZE)) toList
    }
  }

  def returnLargeByteArrays(arrays: List[Array[Byte]]) = Unit
  
}

class Backup(source: File, repository: File, target: String) {
  import Backup._
  
  if (!target.startsWith("/")) throw new IllegalArgumentException("Target path must start with '/'.")
  if (!source.canRead) throw new IllegalArgumentException("Can't read source.")
  
  val connection = Repository.dbConnection(repository)
  val sqlExecutor = SqlDBCommon.executor(1, 100) // currently, 1 thread is best for h2 file db
  val treedb: TreeDBMethods =
    new TreeSqlDB()(connection)
    with TreeDBMethods
    with DeferredInsertTreeDB { val executor = sqlExecutor }
  val datadb: DataInfoDB = new DataInfoSqlDB()(connection)
  val storedb: ByteStoreDB = new ByteStoreSqlDB()(connection)

  // the last part of the target must not exist, the rest is created on demand
  val targetName = treedb nameFromPath target
  val targetParentId = treedb getOrMake (treedb parentPath target)
  if (treedb.childExists(targetParentId, targetName)) throw new IllegalArgumentException("Target already exists.")
  val targetId = treedb create (targetParentId, targetName)
  
  val STOREARRAYSIZE = 1024*1024
  val storeExecutor = SqlDBCommon.executor(4, 1) // 4 threads, do not queue
  def processSource(source: File, parentId: Long): Unit = {
    if (source.isFile) {
      // get print, keep bytes read
      val bytes = new Array[Byte](STOREARRAYSIZE)
      val input = new java.io.RandomAccessFile(source, "r")
      val sizeOfInput = input.length
      val (bytesReadForPrint, print) = PrintCalculator.print(input, bytes)
      // if print matches
      if (datadb.hasMatchingPrint(sizeOfInput, print)) {
        // try to allocate enough memory
        val (fitsToMemory, storeArrays) =
          if (sizeOfInput >= STOREARRAYSIZE) {
            val loaned = loanLargeByteArraysIfPossible(sizeOfInput - STOREARRAYSIZE + 1)
            if (loaned.isEmpty) (false, List(bytes)) else (true, bytes :: loaned)
          } else (true, List(bytes))
        // if fits into memory
        if (fitsToMemory) {
          // read into memory
          // check hash and store if necessary
        } else {
          // does not fit into memory
          // get hash, discard data
          // check hash and store if necessary
        }
        returnLargeByteArrays(storeArrays.tail)
      } else {
        // print does not match
        // store immediately
      }
      
      treedb create (parentId, source.getName, source.lastModified) // FIXME dataid
    } else {
      val id = treedb create (parentId, source.getName)
      source.listFiles.foreach(file => storeExecutor.execute{processSource(file, id)})
    }
  }
  processSource(source, targetId)
  storeExecutor.shutdownAndAwaitTermination
  sqlExecutor.shutdownAndAwaitTermination
  
  throw new AssertionError("not yet fully implemented")
}
package net.diet_rich

import java.io.File
import net.diet_rich.fdfs._
import net.diet_rich.util.closureToRunnable
import net.diet_rich.util.io.readFully


object Backup extends App {
  if (args.length < 3) throw new IllegalArgumentException("Backup needs at least source, repository and target arguments")
  if (args.length > 3) throw new IllegalArgumentException("Too many arguments")
  val source = new File(args(0))
  val repository = new File(args(1))
  val target = args(2)
  new Backup(source, repository, target)

  val MEMORYRESERVED = 1024*1024*64
  val LARGEARRAYSIZE = 1024*1024*2

  def freeMemory: Long = { val rt = Runtime.getRuntime; rt.maxMemory - (rt.totalMemory - rt.freeMemory) }
  
  // FIXME make a real memory management
  def loanLargeByteArraysIfPossible(size: Long): List[Array[Byte]] = {
    if (size > freeMemory + MEMORYRESERVED) Nil else {
      val blocks = (size - 1) / LARGEARRAYSIZE + 1
      (for (i <- 1L to blocks) yield new Array[Byte](LARGEARRAYSIZE)) toList
    }
  }

  def returnLargeByteArrays(arrays: List[Array[Byte]]) = Unit
  
}

class Backup(source: File, repository: File, target: String) {
  import Backup._
  
  // FIXME read from repository settings
  val hashAlgorithm = "MD5"
  
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
    if (!source.isFile) {
      // assuming a directory
      val id = treedb create (parentId, source.getName)
      source.listFiles.foreach(file => storeExecutor.execute{processSource(file, id)})
    } else {
      // immediately start hash calculation
      val input = new HashCalculator(new java.io.RandomAccessFile(source, "r"), "MD5")
      
      // get print, keep bytes read
      // if print does not match, store immediately
      
      // if print matches
      // read as much as possible to memory (one array, max 2G)
      
      // if fits to memory
      // store if necessary
      
      // if does not fit to memory
      // clone hash object
      // read and discard the rest
      // if storing is necessary
      // store from memory
      // seek to read position
      // use cloned hash object on stream
      // read and store
      
      // get byte array iterable, either from random access file, or from memory
      
      
//      // get print, keep bytes read
//      val bytes = new Array[Byte](STOREARRAYSIZE)
//      val sizeOfInput = source.length
//      val input = new HashCalculator(new java.io.RandomAccessFile(source, "r"), "MD5")
//      val (bytesReadForPrint, print) = PrintCalculator.print(input, bytes)
//      // if print matches
//      if (datadb.hasMatchingPrint(sizeOfInput, print)) {
//        // try to allocate enough memory
//        val (fitsToMemory, storeArrays) =
//          if (sizeOfInput >= STOREARRAYSIZE) {
//            val loaned = loanLargeByteArraysIfPossible(sizeOfInput - STOREARRAYSIZE + 1)
//            if (loaned.isEmpty) (false, List(bytes)) else (true, bytes :: loaned)
//          } else (true, List(bytes))
//        // if fits into memory
//        if (fitsToMemory) {
//          // read into memory
//          def readRecurse(startAt: Int, arrays: List[Array[Byte]]): Long = {
//            val array = arrays.head;
//            val read: Long = readFully(input, bytes, startAt, array.length - startAt)
//            read + (if (read == 0) 0 else readRecurse(0, arrays.tail))
//          }
//          readRecurse(bytesReadForPrint, storeArrays)
//          // FIXME continue
//          
//          // check hash and store if necessary
//        } else {
//          // does not fit into memory
//          // get hash, discard data
//          // check hash and store if necessary
//        }
//        returnLargeByteArrays(storeArrays.tail)
//      } else {
//        // print does not match
//        // store immediately
//      }
//      
//      treedb create (parentId, source.getName, source.lastModified) // FIXME dataid
    }
  }
  processSource(source, targetId)
  storeExecutor.shutdownAndAwaitTermination
  sqlExecutor.shutdownAndAwaitTermination
  
  throw new AssertionError("not yet fully implemented")
}
package net.diet_rich

import java.io.File
import net.diet_rich.fdfs._
import net.diet_rich.util.closureToRunnable
import net.diet_rich.util.io.readFully
import net.diet_rich.util.Bytes
import java.io.RandomAccessFile


object Backup extends App {
  if (args.length < 3) throw new IllegalArgumentException("Backup needs at least source, repository and target arguments")
  if (args.length > 3) throw new IllegalArgumentException("Too many arguments")
  val source = new File(args(0))
  val repository = new File(args(1))
  val target = args(2)
  new Backup(source, repository, target)

  
  
  val MEMORYRESERVED = 1024*1024*64 // FIXME system configuration
  val MAXMEMRATIO = 0.75 // FIXME system configuration
  val MINMEMORYCHUNK = 1024 // FIXME system configuration

  def freeMemory: Long = { val rt = Runtime.getRuntime; rt.maxMemory - (rt.totalMemory - rt.freeMemory) }

  /** @return A byte array, if possible of the size indicated, but at least
   *  of the size MINMEMORYCHUNK.
   */
  def applyForLargeByteArray(size: Long): Array[Byte] = {
    val availableMemory = math.max(0, freeMemory - MEMORYRESERVED)
    val calculatedSize = math.min(Int.MaxValue, availableMemory * MAXMEMRATIO).toInt
    val arraySize = math.max(MINMEMORYCHUNK, calculatedSize)
    new Array[Byte](arraySize)
  }

  def readAsMuchAsPossibleToMemory(input: HashCalculatorInput): Bytes = {
    val data = applyForLargeByteArray(input.length - input.position + 1)
    val read = readFully(input, data, 0, data.length)
    Bytes(data, 0, read)
  }
  
}

class HashCalculatorInput(input: RandomAccessFile, algorithm: String) {
  import java.security.MessageDigest
  protected var digester = MessageDigest.getInstance(algorithm)
  protected var markDigester: Option[MessageDigest] = None
  protected var markPosition: Long = 0
  def mark: Unit = {
    markPosition = input.getFilePointer;
    markDigester = Some(digester.clone.asInstanceOf[java.security.MessageDigest])
  }
  def reset = {
    digester = markDigester.get
    input.seek(markPosition)
  }
  def length: Long = input.length
  def position: Long = input.getFilePointer
  def read(bytes: Array[Byte], offset: Int, length: Int): Int = {
    val read = readFully(input, bytes, offset, length)
    digester.update(bytes, offset, read)
    read
  }
  def hash: Array[Byte] = digester.digest
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
  
  val storeExecutor = SqlDBCommon.executor(4, 1) // 4 threads, do not queue
  
  def processSource(source: File, parentId: Long): Unit = {
    if (source.isFile)
      backupFile(source, parentId) 
    else {
      val id = treedb create (parentId, source.getName)
      source.listFiles.foreach(file => storeExecutor.execute{processSource(file, id)})
    }
  }

  def backupFile(source: File, parentId: Long): Unit = {
    val fileSize = source.length
    val input = new HashCalculatorInput(new java.io.RandomAccessFile(source, "r"), "MD5")
    val (fileHeader, print) = PrintCalculator.print(input)

    val dataid = if (datadb.hasMatchingPrint(fileSize, print)) {
      val data = readAsMuchAsPossibleToMemory(input)
      if (data.size < data.maxSize) {
        // if fits to memory, store if necessary
        val size = fileHeader.size.toLong + data.size
        val hash = input.hash
        datadb.findMatch(size, print, hash).getOrElse {
          val dataid = datadb.reserveID
          val method = 0 // FIXME store here fileHeader and Bytes(data, 0, read)
          datadb.create(dataid, DataInfo(size, print, hash, method))
          dataid
        }
      } else {
        // if does not fit to memory, clone hash object, then read and discard the rest.
        // if storing is necessary, store from memory first, then seek to read position,
        // use cloned hash object on stream, read and store.
        input.mark
        val size = readFully(input) + fileHeader.size + data.size
        val hash = input.hash
        datadb.findMatch(size, print, hash).getOrElse {
          val dataid = datadb.reserveID
          val method = 0 // FIXME store here fileHeader and Bytes(data, 0, read)
          // then, seek to right position, use cloned digester, and store the rest
          // this will give a new hash.
          datadb.create(dataid, DataInfo(size, print, hash, method))
          dataid
        }
        0L
      }
    } else {
      0L
    }
      
    treedb create (parentId, source.getName, source.lastModified, dataid)
      // get print, keep bytes read
      // if print does not match, store immediately
      
      
      // store if necessary
      
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

  processSource(source, targetId)
  storeExecutor.shutdownAndAwaitTermination
  sqlExecutor.shutdownAndAwaitTermination
  
  throw new AssertionError("not yet fully implemented")
}
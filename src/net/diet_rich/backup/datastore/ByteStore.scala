// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.datastore

import akka.actor.{TypedActor,TypedActorConfiguration}
import akka.config.Configuration
import akka.dispatch.{BoundedMailbox,Dispatchers}
import java.io.{File, RandomAccessFile}

case class Bytes(bytes: Array[Byte], length: Int, offset: Int = 0) {
  require(offset >= 0)
  require(length >= 0)
  require(bytes.length >= offset + length)
  def dropFirst(size: Int) = copy(length = length - size, offset = offset + size)
  def keepFirst(size: Int) = copy(length = size)
}

object Bytes {
  def apply(bytes: Array[Byte]) : Bytes = Bytes(bytes, bytes.length)
  def apply(size: Int) : Bytes = Bytes(new Array[Byte](size))
}

object ByteStore {
  private val dispatcher = Dispatchers
      .newExecutorBasedEventDrivenDispatcher("bounded mailbox dispatcher", 0, BoundedMailbox(1))
      .build
  private val typedActorConfig = TypedActorConfiguration().dispatcher(dispatcher)
  
  def apply(configuration: Configuration) : ByteStore =
    TypedActor.newInstance(classOf[ByteStore], new ByteStoreImpl(configuration), typedActorConfig)
}

trait ByteStore {
  /** May throw an exception. */
  def close : Unit
  /** May throw an exception. */
  def readBytes(position: Long, length: Int) : Array[Byte]
  /** May throw an exception. */
  def writeBytes(position: Long, bytes: Bytes) : Unit
}

class ByteStoreImpl(config: Configuration) extends TypedActor with ByteStore {
  
  override def close : Unit = {
    self.stop()
    fileAccessorCache.values.foreach(_.close())
  }
  override def readBytes(position: Long, length: Int) : Array[Byte] = new Array[Byte](0) // FIXME 
  def writeBytes(position: Long, bytes: Bytes) : Unit = Unit // FIXME
  
  val sqrtOfFilesPerFolder: Int     = config("ByteStore.sqrtOfFilesPerFolder", 20)
  val accessorCacheSize   : Int     = config("ByteStore.accessorCacheSize",    10)
  val readonly            : Boolean = config("ByteStore.readonly",             false)
  val fileSize            : Long    = config("ByteStore.fileSize",             0x400000)
  val storeDirectory      : String  = config("ByteStore.storeDirectory")
  
  val filesPerFolder = sqrtOfFilesPerFolder * sqrtOfFilesPerFolder
  val fileAccessorCache = collection.mutable.LinkedHashMap.empty[File, RandomAccessFile]

  private def relativeFilePath(fileNumber: Long) : String =
    "%015d/%015.bck".formatLocal(java.util.Locale.US, fileNumber - fileNumber % filesPerFolder, fileNumber)

  private def fileAndPosition(position: Long) : (File, Long) = {
    val fileNumber = position / fileSize
    val positionInFile = position % fileSize
    (new File(storeDirectory, relativeFilePath(fileNumber)), positionInFile)
  }
  
  private def fileAccessor(file: File) : RandomAccessFile =
    fileAccessorCache.getOrElse(file, {
      if (fileAccessorCache.size > accessorCacheSize) {
        val (file, accessor) = fileAccessorCache.head
        accessor.close
        fileAccessorCache -= file
      }
      val accessor = new RandomAccessFile(file, if (readonly) "r" else "rw")
      fileAccessorCache += file -> accessor
      accessor
    })

  private def accessorAndPosition(position: Long) : (RandomAccessFile, Long) = {
      val (file, positionInFile) = fileAndPosition(position)
      val accessor = fileAccessor(file)
      accessor.seek(positionInFile)
      (accessor, positionInFile)
  }
    
  /** Blocks until requested length is read or end of stream is reached.
   * 
   *  @return the number of bytes read.
   */
  private def readFromAccessor(accessor: RandomAccessFile, bytes: Bytes) : Int = {
    @annotation.tailrec
    def readBytesTailRec(bytes: Bytes) : Int =
      accessor.read(bytes.bytes, bytes.offset, bytes.length) match {
        case bytesRead if bytesRead <= 0 => bytes.length
        case bytesRead => readBytesTailRec(bytes.dropFirst(bytesRead))
      }
    bytes.length - readBytesTailRec(bytes)
  }

  private def internalWriteBytes(position: Long, bytes: Bytes) : Unit = {
    @annotation.tailrec
    def writeBytesTailRec(position: Long, bytes: Bytes) : Unit = {
      val (accessor, positionInFile) = accessorAndPosition(position)
      if (positionInFile + bytes.length > fileSize) {
        val bytesToWrite = (fileSize - positionInFile).toInt
        accessor.write(bytes.bytes, bytes.offset, bytesToWrite)
        writeBytesTailRec(position + bytesToWrite, bytes.dropFirst(bytesToWrite))
      } else {
        accessor.write(bytes.bytes, bytes.offset, bytes.length)
      }
    }
    writeBytesTailRec(position, bytes)
  }

  /** Fills parts missing because data file is too short with 0. */
  private def internalReadBytes(position: Long, length: Int) : Array[Byte] = {
    @annotation.tailrec
    def readBytesTailRec(position: Long, bytes: Bytes) : Unit = {
      val (accessor, positionInFile) = accessorAndPosition(position)
      if (positionInFile + length > fileSize) {
        val bytesToRead = (fileSize - positionInFile).toInt
        readFromAccessor(accessor, bytes.keepFirst(bytesToRead))
        readBytesTailRec(position + bytesToRead, bytes.dropFirst(bytesToRead))
      } else {
        readFromAccessor(accessor, bytes)
      }
    }
    val bytes = Bytes(length)
    readBytesTailRec(position, bytes)
    bytes.bytes
  }
}



//import java.io.{File,RandomAccessFile}
//import scala.actors.Actor._
//import scala.actors.Future
//
//
//
//
//
//trait StoreAccess {
//  def close : Option[Throwable]
//  def readBytes(position: Long, length: Int) : Either[Throwable,Array[Byte]]
//  def writeBytes(
//      position: Long, 
//      bytes: Array[Byte],
//      length: Int = Int.MaxValue,
//      offset: Int = 0) : Future[Option[Throwable]]
//}
//
//abstract class LimitedQueueActor(val queueSize : Int) {
//  private val semaphore = new java.util.concurrent.Semaphore(queueSize, true)
//  private val actor = scala.actors.Actor.actor { act }
//
//  def !? (msg: Any): Any         = { semaphore.acquire ; actor !? msg }
//  def !! (msg: Any): Future[Any] = { semaphore.acquire ; actor !! msg }
//  
//  protected def reply (msg: Any): Unit = { semaphore.release ; scala.actors.Actor.reply(msg) }
//  protected def act() : Unit
//}
//
//// EVENTUALLY add requirements to the methods
//class StoreAccessImpl(storeDir: File, fileSize: Long) extends StoreAccess {
//
//  override def close : Option[Throwable] =
//    (storeActor !! "EXIT").asInstanceOf[Option[Throwable]]
//  
//  override def readBytes(position: Long, length: Int) : Either[Throwable,Array[Byte]] =
//    (storeActor !? ((position, length))).asInstanceOf[Either[Throwable,Array[Byte]]]
//
//  override def writeBytes(position: Long, bytes: Array[Byte], length: Int, offset: Int) : Future[Option[Throwable]] =
//    (storeActor !! ((position, bytes, math.min(length, bytes.length), offset))).asInstanceOf[Future[Option[Throwable]]]
//
//  val queueSize = 10 // EVENTUALLY make configurable
//  val sqrtOfFilesPerFolder = 20 // EVENTUALLY make configurable
//  val fileMapMaxSize = 20 // EVENTUALLY make configurable
//  val readonly = false // EVENTUALLY make configurable
//  
//  val filesPerFolder = sqrtOfFilesPerFolder * sqrtOfFilesPerFolder
//  val fileAccessorCache = collection.mutable.LinkedHashMap.empty[File, RandomAccessFile]
//
//  /** Note: Ideally, this actor should be FIFO (which scala's actors not
//   *  always are) and its mailbox should be bound and blocking (which is 
//   *  implemented using the semaphore). Possibly, the actors of the akka 
//   *  framework would offer these features out of the shelf.
//   */
//  private val storeActor = new LimitedQueueActor(queueSize) {
//    override def act {
//      loop { react {
//        case (position: Long, length: Int) =>
//          reply(internalReadBytes(position, length))
//        case (position: Long, bytes: Array[Byte], length: Int, offset: Int) =>
//          reply(internalWriteBytes(position, bytes, length, offset))
//        case _ =>
//          reply(internalClose) ; exit
//      } }
//    }
//  }
//
//  private def internalClose = 
//    fileAccessorCache.values.foreach(_.close())
//  
//  private def pathForFileNumber(fileNumber: Long) : String =
//    "%015d/%015.bck".formatLocal(java.util.Locale.US, fileNumber - fileNumber % filesPerFolder, fileNumber)
//
//  private def fileAndPosition(position: Long) : (File, Long) = {
//    val fileNumber = position / fileSize
//    val positionInFile = position % fileSize
//    (new File(storeDir, pathForFileNumber(fileNumber)), positionInFile)
//  }
//  
//  private def fileAccessor(file: File) : RandomAccessFile =
//    fileAccessorCache.getOrElse(file, {
//      if (fileAccessorCache.size > fileMapMaxSize) {
//        val (file, accessor) = fileAccessorCache.head
//        accessor.close
//        fileAccessorCache -= file
//      }
//      val accessor = new RandomAccessFile(file, if (readonly) "r" else "rw")
//      fileAccessorCache += file -> accessor
//      accessor
//    })
//
//  private def accessorAndPosition(position: Long) : (RandomAccessFile, Long) = {
//      val (file, positionInFile) = fileAndPosition(position)
//      val accessor = fileAccessor(file)
//      accessor.seek(positionInFile)
//      (accessor, positionInFile)
//  }
//    
//  /** Blocks until requested length is read or end of stream is reached.
//   * 
//   *  @return the number of bytes read.
//   */
//  private def readFromAccessor(accessor: RandomAccessFile, buffer: Array[Byte], offset: Int, length: Int) : Int = {
//    require(offset >= 0)
//    require(length >= 0)
//    require(buffer.length >= offset + length)
//    
//    @annotation.tailrec
//    def readBytesTailRec(alreadyRead: Int) : Int =
//      accessor.read(buffer, offset + alreadyRead, length - alreadyRead) match {
//        case bytesRead if bytesRead <= 0 => alreadyRead
//        case bytesRead => readBytesTailRec(alreadyRead + bytesRead)
//      }
//    readBytesTailRec(0)
//  }
//  
//  private def internalWriteBytes(position: Long, bytes: Array[Byte], length: Int, offset: Int) : Option[Throwable] = {
//    try {
//      @annotation.tailrec
//      def writeBytesTailRec(position: Long, bytes: Array[Byte], length: Int, offset: Int) : Unit = {
//        val (accessor, positionInFile) = accessorAndPosition(position)
//        if (positionInFile + length > fileSize) {
//          val bytesToWrite = (fileSize - positionInFile).toInt
//          accessor.write(bytes, offset, bytesToWrite)
//          writeBytesTailRec(position+bytesToWrite, bytes, length - bytesToWrite, offset + bytesToWrite)
//        } else {
//          accessor.write(bytes, offset, length)
//        }
//      }
//      writeBytesTailRec(position, bytes, length, offset)
//      None
//    } catch { case e => Some(e) }
//  }
//
//  private def internalReadBytes(position: Long, length: Int) : Either[Throwable,Array[Byte]] = {
//    try {
//      @annotation.tailrec
//      def readBytesTailRec(bytes: Array[Byte], position: Long, offset: Int, length: Int) : Unit = {
//        val (accessor, positionInFile) = accessorAndPosition(position)
//        if (positionInFile + length > fileSize) {
//          val bytesToRead = (fileSize - positionInFile).toInt
//          readFromAccessor(accessor, bytes, offset, bytesToRead)
//          readBytesTailRec(bytes, position + bytesToRead, offset + bytesToRead, length - bytesToRead)
//        } else {
//          readFromAccessor(accessor, bytes, offset, length)
//        }
//      }
//      val result = new Array[Byte](length)
//      readBytesTailRec(result, position, 0, length)
//      Right(result)
//    } catch { case e => Left(e) }
//  }
//
//}

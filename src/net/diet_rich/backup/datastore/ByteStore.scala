// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.datastore

import akka.actor.{TypedActor,TypedActorConfiguration}
import akka.config.Configuration
import akka.dispatch.{BoundedMailbox,Dispatchers,Future}
import java.io.{File, RandomAccessFile}
import net.diet_rich.backup.BackupSystemConfig

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
  /* Note: The actors in the backup system must have the following attributes:
   * 
   * a) They must have a bounded mailbox to avoid running out of memory due to
   *    large messages queuing up in the system.
   * b) They must guarantee that messages sent from one thread to one actor are
   *    processed FIFO.
   */
  val mailboxSizeLimit = BackupSystemConfig()("actors.MailboxSizeLimit", 20)
  val boundedMailboxDispatcher = Dispatchers
      .newExecutorBasedEventDrivenDispatcher("bounded mailbox dispatcher", 0, BoundedMailbox(mailboxSizeLimit))
      .build
  val boundedTypedActorConfig = TypedActorConfiguration().dispatcher(boundedMailboxDispatcher)
  
  def apply(configuration: Configuration) : ByteStore =
    TypedActor.newInstance(classOf[ByteStore], new ByteStoreImpl(configuration), boundedTypedActorConfig)
}

trait ByteStore {
  /** Fills areas nothing has been written to with 0. May throw an exception. */
  def readBytes(position: Long, length: Int) : Array[Byte]
  def writeBytes(position: Long, bytes: Bytes) : Future[Option[Throwable]]
  def close : Future[List[Throwable]]
}

/** Used only by clients in net.diet_rich.backup.datastore. */
class ByteStoreImpl(config: Configuration) extends TypedActor with ByteStore {
  
  override def close : Future[List[Throwable]] = {
    self.stop()
    Future(
        fileAccessorCache.values.
        foldLeft(List[Throwable]())((list,accessor) => 
          try { accessor.close() ; list } catch { case e => e :: list }
        ))
  }
  
  override def readBytes(position: Long, length: Int) : Array[Byte] =
    internalReadBytes(position, length)
  
  def writeBytes(position: Long, bytes: Bytes) : Future[Option[Throwable]] =
    try { internalWriteBytes(position, bytes) ; Future(None) } catch { case e => Future(Some(e)) }
  
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

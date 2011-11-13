// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.datastore

import akka.actor.{TypedActor,TypedActorConfiguration}
import akka.config.Configuration
import akka.dispatch.{BoundedMailbox,Dispatchers,Future}
import java.io.File
import net.diet_rich.backup.BackupSystemConfig
import net.diet_rich.util.Bytes
import net.diet_rich.util.io.RandomAccessFile

object ByteStore {
  /* Note: The actors in the backup system must have the following attributes:
   * 
   * a) They must have a bounded mailbox to avoid running out of memory due to
   *    large messages queuing up in the system.
   * b) They must guarantee that messages sent from one thread to one actor are
   *    processed FIFO.
   */
  private val mailboxSizeLimit = BackupSystemConfig()("actors.MailboxSizeLimit", 8)
  val boundedMailboxDispatcher = Dispatchers
      .newExecutorBasedEventDrivenDispatcher("bounded mailbox dispatcher", 0, BoundedMailbox(mailboxSizeLimit))
      .build
  val boundedTypedActorConfig = TypedActorConfiguration().dispatcher(boundedMailboxDispatcher)
  
  def apply(configuration: Configuration) : ByteStore =
    TypedActor.newInstance(classOf[ByteStore], new ByteStoreImpl(configuration), boundedTypedActorConfig)
    
  def readonly(configuration: Configuration) : ByteStoreReadOnly =
    TypedActor.newInstance(classOf[ByteStoreReadOnly], new ByteStoreImplReadOnly(configuration), boundedTypedActorConfig)
}

trait ByteStoreReadOnly {
  /** Fills areas nothing has been written to with 0. May throw an exception. */
  def readBytes(position: Long, length: Int) : Bytes
  def close : Future[List[Throwable]]
}

trait ByteStore extends ByteStoreReadOnly {
  def writeBytes(position: Long, bytes: Bytes) : Future[Option[Throwable]]
}

/** Used only by clients in net.diet_rich.backup.datastore. */
class ByteStoreImplReadOnly(config: Configuration) extends TypedActor with ByteStoreReadOnly {
  
  // FIXME make truly read-only
  
  override def close : Future[List[Throwable]] = {
    self.stop()
    Future(
        fileAccessorCache.values.
        foldLeft(List[Throwable]())((list,accessor) => 
          try { accessor.close ; list } catch { case e => e :: list }
        ))
  }
  
  override def readBytes(position: Long, length: Int) : Bytes =
    processBytesTailRec(position, Bytes(length)){ (accessor, bytes) => accessor.readFully(bytes) }
  
  val readonly            : Boolean = true
  val sqrtOfFilesPerFolder: Int     = config("ByteStore.sqrtOfFilesPerFolder", 20)
  val accessorCacheSize   : Int     = config("ByteStore.accessorCacheSize",    10)
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
      val accessor = new RandomAccessFile(file)
      fileAccessorCache += file -> accessor
      accessor
    })

  private def accessorAndPosition(position: Long) : (RandomAccessFile, Long) = {
      val (file, positionInFile) = fileAndPosition(position)
      val accessor = fileAccessor(file)
      accessor.seek(positionInFile)
      (accessor, positionInFile)
  }
    
  @annotation.tailrec
  protected final def processBytesTailRec(position: Long, bytes: Bytes)(task: (RandomAccessFile,Bytes) => Unit) : Bytes = {
    require(position >= 0)
    val (accessor, positionInFile) = accessorAndPosition(position)
    if (positionInFile + bytes.length > fileSize) {
      val bytesToWrite = (fileSize - positionInFile).toInt
      task(accessor, bytes.keepFirst(bytesToWrite))
      processBytesTailRec(position + bytesToWrite, bytes.dropFirst(bytesToWrite))(task)
    } else {
      task(accessor, bytes)
      bytes
    }
  }  
}

/** Used only by clients in net.diet_rich.backup.datastore. */
class ByteStoreImpl(config: Configuration) extends ByteStoreImplReadOnly(config) with ByteStore {
  override val readonly : Boolean = false
  
  def writeBytes(position: Long, bytes: Bytes) : Future[Option[Throwable]] =
    try { 
      processBytesTailRec(position, bytes){ (accessor, bytes) => accessor.write(bytes) }
      Future(None) 
    } catch { case e => Future(Some(e)) }
}

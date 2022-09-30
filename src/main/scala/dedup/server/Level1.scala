package dedup
package server

import java.util.concurrent.atomic.AtomicLong

class Level1(settings: Settings) extends AutoCloseable with util.ClassLogging:

  val backend: Level2 = Level2(settings)
  export backend.{child, children, entry, mkDir, setTime, split, update}

  /** Creates a copy of the file's last persisted state without current modifications. */
  def copyFile(file: FileEntry, newParentId: Long, newName: String): Boolean = 
    backend.mkFile(newParentId, newName, file.time, file.dataId).isDefined

  /** id -> (handle count, dataEntry). Remember to synchronize. */
  private var files = Map[Long, (Int, DataEntry)]()

  def delete(entry: TreeEntry): Unit =
    watch(s"delete($entry)") {
      // From man unlink(2)
      // If the name was the last link to a file but any processes still have the file open,
      // the file will remain in existence until the last file descriptor referring to it is closed.
      // ... This means that we keep the file open in level 1 if it currently is open ...
      backend.delete(entry.id)
    }

  def createAndOpen(parentId: Long, name: String, time: Time): Option[Long] =
    watch(s"createAndOpen(parentId = $parentId, name = '$name')") {
      // A sensible Option.tapEach will be available in Scala 3.1, see
      // https://stackoverflow.com/questions/67017901/why-does-scala-option-tapeach-return-iterable-not-option
      // and https://github.com/scala/scala-library-next/pull/80
      backend.mkFile(parentId, name, time, DataId(-1)).tap(_.foreach { id =>
        synchronized(files += id -> (1, DataEntry(AtomicLong(-1), 0, settings.tempPath)))
      })
    }

  def open(file: FileEntry): Unit =
    watch(s"open($file)") {
      synchronized { import file.*
        files += id -> (files.get(id) match
          case None => 1 -> backend.newDataEntry(id, dataId)
          case Some(count -> dataEntry) => count + 1 -> dataEntry
        )
      }
    }

  def size(file: FileEntry): Long =
    watch(s"size($file)") {
      synchronized(files.get(file.id)).map(_._2.size).getOrElse(backend.size(file))
    }

  def truncate(id: Long, newSize: Long): Boolean =
    watch(s"truncate(id: $id, newSize: $newSize)") {
      synchronized(files.get(id)).map(_._2.truncate(newSize)).isDefined
    }

  /** @param data Iterator(position -> bytes). Providing the complete data as Iterator allows running the update
    *             atomically / synchronized. Note that the byte arrays may be kept in memory, so make sure e.g.
    *             using defensive copy (Array.clone) that they are not modified later.
    * @return `false` if called without createAndOpen or open. */
  def write(id: Long, data: Iterator[(Long, Array[Byte])]): Boolean =
    watch(s"write(id: $id, data: Iterator...)") {
      synchronized(files.get(id)).map(_._2.write(data)).isDefined
    }
  
  /** Reads bytes from the referenced file and writes them to `sink`.
    * Reads the requested number of bytes unless end-of-file is reached
    * first, in that case stops there.
    *
    * Note: Providing a `sink` instead of returning the data enables
    * atomic reads in the - at [[Level1]] mutable - [[DataEntry]] without
    * incurring the risk of large memory allocations.
    *
    * @param id     id of the file to read from.
    * @param offset offset in the file to start reading at.
    * @param size   number of bytes to read, NOT limited by the internal size limit for byte arrays.
    * @param sink   sink to write data to, starting at sink position 0.
    *
    * @return Some(actual size read) or None if called without createAndOpen.
    */
  def read[D: DataSink](id: Long, offset: Long, size: Long, sink: D): Option[Long] =
    watch(s"read(id = $id, offset = $offset, size = $size)") {
      synchronized(files.get(id)).map { (_, dataEntry) =>
        val sizeRead -> holes = dataEntry.read(offset, size, sink)
        holes.foreach { (holeOffset, holeSize) =>
          backend.read(id, dataEntry.getBaseDataId, holeOffset, holeSize)
            .foreach { (dataOffset, data) => sink.write(dataOffset - offset, data) }
        }
        sizeRead
      }
    }

  def release(id: Long): Boolean =
    watch(s"release($id)") {
      val result = synchronized(files.get(id) match {
        case None =>
          log.warn(s"release($id) called for a file handle that is currently not open.")
          None // No handle found, will return false.
        case Some(count -> dataEntry) =>
          if count < 0 then log.error(s"Handle count $count for id $id")
          if count > 1 then { files += id -> (count - 1, dataEntry); Some(None) } // Nothing else to do - file is still open.
          else { files -= id; Some(Some(dataEntry)) } // Outside the sync block persist data if necessary.
      })
      result.flatten.foreach(data => if data.written then backend.persist(id, data) else data.close(DataId(-1)))
      result.isDefined
    }

  override def close(): Unit = synchronized {
    if files.nonEmpty then
      log.warn(s"Forcibly closing ${files.size} open files.")
      files.foreach { case (id, (_, data)) => if data.written then backend.persist(id, data) }
    backend.close()
  }

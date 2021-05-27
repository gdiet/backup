package dedup
package server

import java.util.concurrent.atomic.AtomicLong

class Level1(settings: Settings) extends AutoCloseable with util.ClassLogging:

  val backend = Level2(settings)
  export backend.{child, children, close, mkDir, setTime, update}

  def split(path: String)       : Array[String]     = path.split("/").filter(_.nonEmpty)
  def entry(path: String)       : Option[TreeEntry] = entry(split(path))
  def entry(path: Array[String]): Option[TreeEntry] = path.foldLeft(Option[TreeEntry](root)) {
                                                        case (Some(dir: DirEntry), name) => child(dir.id, name)
                                                        case _ => None
                                                      }
                                                      
  /** Creates a copy of the file's last persisted state without current modifications. */
  def copyFile(file: FileEntry, newParentId: Long, newName: String): Boolean = 
    backend.mkFile(newParentId, newName, file.time, file.dataId).isDefined

  /** id -> (handle count, dataEntry). Remember to synchronize. */
  private var files = Map[Long, (Int, DataEntry)]()

  def delete(entry: TreeEntry): Unit =
    watch(s"delete($entry)") {
      // TODO delete from Level1
      backend.delete(entry.id)
    }

  def createAndOpen(parentId: Long, name: String, time: Time): Option[Long] =
    watch(s"createAndOpen(parentId = $parentId, name = '$name')") {
      // https://stackoverflow.com/questions/67017901/why-does-scala-option-tapeach-return-iterable-not-option
      // TODO use https://github.com/scala/scala-library-next/pull/80
      backend.mkFile(parentId, name, time, DataId(-1)).tap(_.foreach { id =>
        synchronized(files += id -> (1, DataEntry(new AtomicLong(-1), 0, settings.tempPath)))
      })
    }

  def size(id: Long, dataId: DataId): Long = 0 // FIXME

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
      synchronized(files.get(id)).map { case _ -> dataEntry =>
        val sizeRead -> holes = dataEntry.read(offset, size, sink)
        holes.foreach { case holeOffset -> holeSize =>
          backend.read(id, dataEntry.baseDataId.get(), holeOffset, holeSize)
            .foreach { case dataOffset -> data => sink.write(dataOffset - offset, data) }
        }
        sizeRead
      }
    }
 
end Level1

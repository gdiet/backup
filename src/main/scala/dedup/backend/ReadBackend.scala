package dedup
package backend

import dedup.db.ReadDatabase
import dedup.server.Settings
import dedup.store.LongTermStore
import dedup.util.ClassLogging
import dedup.{DirEntry, FileEntry, TreeEntry}

import scala.collection.immutable.LazyList

class ReadBackend(settings: Settings, db: ReadDatabase) extends Backend with ClassLogging:

  protected final val lts: LongTermStore = store.LongTermStore(settings.dataDir, settings.readonly)
  protected final val handlesRead: FileHandlesRead = FileHandlesRead()

  override def shutdown(): Unit =
    // On Windows, it's sort of normal to still have read file handles open when shutting down the file system.
    val handleCount = handlesRead.shutdown()
    if handleCount > 0 then log.debug(s"Still $handleCount open read handles when unmounting the file system.")
    lts.close()

  // *** Tree and meta data operations ***
  override def size(fileEntry: FileEntry): Long = db.logicalSize(fileEntry.dataId)
  override final def children(parentId: Long): Seq[TreeEntry] = db.children(parentId)
  override final def child(parentId: Long, name: String): Option[TreeEntry] = db.child(parentId, name)
  override final def entry(path: Array[String]): Option[TreeEntry] =
    path.foldLeft(Option[TreeEntry](root)) {
      case (Some(dir: DirEntry), name) => db.child(dir.id, name)
      case _ => None
    }

  // *** File content operations ***
  def open(fileId: Long, dataId: DataId): Unit = handlesRead.open(fileId, dataId)

  def release(fileId: Long): Option[DataId] = handlesRead.release(fileId)

  override def read(fileId: Long, offset: Long, requestedSize: Long): Option[Iterator[(Long, Array[Byte])]] = {
    handlesRead.dataId(fileId).map { dataId =>
      val fileSize -> parts = db.logicalSize(dataId) -> db.parts(dataId)
      readFromLts(parts, offset, math.min(requestedSize, fileSize - offset))
    }
  }

  /** From the long term store, reads file content defined by `parts`.
    *
    * @param parts    List of (offset, size) defining the file content parts to read.
    *                 `readFrom` + `readSize` should not exceed summed part sizes unless
    *                 `parts` is the empty list that is used for blacklisted entries.
    * @param readFrom Position in the file to start reading at, must be >= 0.
    * @param readSize Number of bytes to read, must be >= 0.
    * @return A contiguous Iterator(position, bytes) where data chunk size is limited to [[dedup.memChunk]].
    *         If `parts` is too small, the data is filled up with zeros.
    * @throws IllegalArgumentException if `readFrom` is negative or `readSize` is less than 1.
    */
  protected def readFromLts(parts: Seq[(Long, Long)], readFrom: Long, readSize: Long): Iterator[(Long, Array[Byte])] =
    log.trace(s"readFromLts(readFrom: $readFrom, readSize: $readSize, parts: $parts)")
    ensure("read.lts.offset", readFrom >= 0, s"Read offset $readFrom must be >= 0.")
    ensure("read.lts.size", readSize >= 0, s"Read size $readSize must be > 0.")
    val partsToReadFrom = parts.foldLeft(0L -> Vector[(Long, Long)]()) {
      case ((currentOffset, result), part@(partPosition, partSize)) =>
        val distance = readFrom - currentOffset
        if distance > partSize then currentOffset + partSize -> result
        else if distance > 0 then currentOffset + partSize -> (result :+ (partPosition + distance, partSize - distance))
        else currentOffset + partSize -> (result :+ part)
    }._2
  
    def recurse(remainingParts: Seq[(Long, Long)], readSize: Long, resultOffset: Long): LazyList[(Long, Array[Byte])] =
      remainingParts match
        case Seq() =>
          if parts.nonEmpty then log.warn(s"Could not fully read $readSize bytes starting at $readFrom from these parts: $parts")
          LazyList.range(resultOffset, readSize, memChunk.toLong).map(
            offset => offset -> new Array[Byte](math.min(memChunk, readSize - offset).toInt)
          )
        case (partPosition, partSize) +: rest =>
          if partSize < readSize then lts.read(partPosition, partSize, resultOffset) #::: recurse(rest, readSize - partSize, resultOffset + partSize)
          else lts.read(partPosition, readSize, resultOffset)
  
    recurse(partsToReadFrom, readSize, readFrom).iterator

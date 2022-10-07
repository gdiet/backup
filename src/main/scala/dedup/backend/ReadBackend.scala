package dedup
package backend

import dedup.db.{ReadDatabase, WriteDatabase}
import dedup.server.{DataSink, Settings}
import dedup.util.ClassLogging
import dedup.{DirEntry, FileEntry, TreeEntry}

import scala.collection.immutable.LazyList

/** Don't instantiate more than one backend for a repository. */
// Why not? Because the backend object is used for synchronization.
class ReadBackend(settings: Settings, db: ReadDatabase) extends Backend with ClassLogging:

  // TODO also implement shutdown here and log warn the number of open file handles if any
  // TODO and close the lts
  //  def shutdown(): Unit = sync { db.shutdownCompact() }

  /** file id -> (handle count, data id). Remember to synchronize. */
  private var files = Map[Long, (Int, DataId)]()

  private val lts = store.LongTermStore(settings.dataDir, settings.readonly)


  // *** Tree and meta data operations ***
  
  override def size(fileEntry: FileEntry): Long = sync { db.logicalSize(fileEntry.dataId) }
  
  override final def children(parentId: Long): Seq[TreeEntry] = sync { db.children(parentId) }

  override final def entry(path: Array[String]): Option[TreeEntry] =
    path.foldLeft(Option[TreeEntry](root)) {
      case (Some(dir: DirEntry), name) => sync { db.child(dir.id, name) }
      case _ => None
    }

  // *** File content operations ***

  def open(file: FileEntry): Unit = sync {
    files += file.id -> (files.get(file.id) match
      case None => 1 -> file.dataId
      case Some(count -> dataId) =>
        ensure("readbackend.open", file.dataId == dataId, s"Open #$count - dataId ${file.dataId} differs from previous $dataId.")
        count + 1 -> dataId
    )
  }

  def release(fileId: Long): Boolean = sync {
    files.get(fileId) match
      case None =>
        log.warn(s"release($fileId) called for a file handle that is currently not open.")
        false
      case Some(count -> dataId) =>
        if count < 0 then log.error(s"Handle count $count for id $fileId.")
        if count > 1 then files += fileId -> (count - 1, dataId) else files -= fileId
        true
  }

  override def read[D: DataSink](fileId: Long, offset: Long, requestedSize: Long, sink: D): Option[Long] = {
    sync(files.get(fileId)).map { case (_, dataId) =>
      val (fileSize -> parts) = sync(db.logicalSize(dataId) -> db.parts(dataId))
      readFromLts(parts, offset, math.min(requestedSize, fileSize - offset))
        .map { case (position, data) =>
          log.info(s"writing... at $position size ${data.length}")
          sink.write(position - offset, data); data.length.toLong }
        .sum
    }
  }

  /** Reads bytes from the long term store from a file defined by `parts`.
    * Reads the requested number of bytes unless end-of-file is reached
    * first, in that case stops there.
    *
    * @param parts    List of (offset, size) defining the parts of the file to read from.
    *                 For blacklisted entries, can be the empty list.
    * @param readFrom Position in the file to start reading at, must be >= 0.
    * @param readSize Number of bytes to read. Any number < 1 results in an empty Iterator.
    * @return A contiguous Iterator(position, bytes) where data chunk size is limited to [[dedup.memChunk]].
    * @throws IllegalArgumentException if `readFrom` or `readSize` exceed the bounds defined by `parts`.
    */
  protected def readFromLts(parts: Seq[(Long, Long)], readFrom: Long, readSize: Long): Iterator[(Long, Array[Byte])] =
    if readSize < 1 then Iterator.empty
    else if parts.isEmpty then // Read appropriate number of zeros from blacklisted entry.
      Iterator.range(0L, readSize, memChunk.toLong)
      .map { offset => readFrom + offset -> new Array[Byte](math.min(memChunk, readSize - offset).toInt) }
    else
      ensure("read.lts.offset", readFrom >= 0, s"Read offset $readFrom must be >= 0.")
      val partsToReadFrom = parts.foldLeft(0L -> Vector[(Long, Long)]()) {
        case ((currentOffset, result), part@(partPosition, partSize)) =>
          val distance = readFrom - currentOffset
          if distance > partSize then currentOffset + partSize -> result
          else if distance > 0 then currentOffset + partSize -> (result :+ (partPosition + distance, partSize - distance))
          else currentOffset + partSize -> (result :+ part)
      }._2

      def recurse(remainingParts: Seq[(Long, Long)], readSize: Long, resultOffset: Long): LazyList[(Long, Array[Byte])] =
        val (partPosition, partSize) +: rest = remainingParts
        if partSize >= readSize then lts.read(partPosition, readSize, resultOffset)
        else
          lts.read(partPosition, partSize, resultOffset) #::: {
            if rest.isEmpty then LazyList.empty else recurse(rest, readSize - partSize, resultOffset + partSize)
          }

      recurse(partsToReadFrom, readSize, readFrom).iterator

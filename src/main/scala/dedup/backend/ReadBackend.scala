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
  //  def shutdown(): Unit = sync { db.shutdownCompact() }

  /** id -> (handle count, dataEntry). Remember to synchronize. */
  private var files = Map[Long, (Int, DataId)]()

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

  override def read[D: DataSink](fileId: Long, offset: Long, size: Long, sink: D): Option[Long] = {
    sync(files.get(fileId)).map { case (_, dataId) =>
      val parts = sync(db.parts(dataId))
      readFromLts(parts, offset, size)
        .map { case (dataOffset, data) => sink.write(dataOffset - offset, data); data.length.toLong }
        .sum
    }
  }

  /** Reads bytes from the long term store from a file defined by `parts`.
    *
    * @param parts    List of (offset, size) defining the parts of the file to read from.
    *                 `readFrom` + `readSize` must not exceed summed part sizes unless
    *                 `parts` is the empty list that is used for blacklisted entries.
    * @param readFrom Position in the file to start reading at, must be >= 0.
    * @param readSize Number of bytes to read, must be >= 0.
    * @return A contiguous Iterator(position, bytes) where data chunk size is limited to [[dedup.memChunk]].
    * @throws IllegalArgumentException if `readFrom` or `readSize` exceed the bounds defined by `parts`.
    */
  protected def readFromLts(parts: Seq[(Long, Long)], readFrom: Long, readSize: Long): Iterator[(Long, Array[Byte])] =
    ???
//    if parts.isEmpty then // Read appropriate number of zeros from blacklisted entry.
//      Iterator.range(0L, readSize, memChunk.toLong).map(
//        offset => readFrom + offset -> new Array[Byte](math.min(memChunk, readSize - offset).toInt)
//      )
//    else
//      log.trace(s"readFromLts(parts: $parts, readFrom: $readFrom, readSize: $readSize)")
//      ensure("read.lts.offset", readFrom >= 0, s"Read offset $readFrom must be >= 0.")
//      ensure("read.lts.size", readSize > 0, s"Read size $readSize must be > 0.")
//      val (lengthOfParts, partsToReadFrom) = parts.foldLeft(0L -> Vector[(Long, Long)]()) {
//        case ((currentOffset, result), part@(partPosition, partSize)) =>
//          val distance = readFrom - currentOffset
//          if distance > partSize then currentOffset + partSize -> result
//          else if distance > 0 then currentOffset + partSize -> (result :+ (partPosition + distance, partSize - distance))
//          else currentOffset + partSize -> (result :+ part)
//      }
//      ensure("read.lts.parts", lengthOfParts >= readFrom + readSize, s"Read offset $readFrom size $readSize exceeds parts length $parts.")
//
//      def recurse(remainingParts: Seq[(Long, Long)], readSize: Long, resultOffset: Long): LazyList[(Long, Array[Byte])] =
//        val (partPosition, partSize) +: rest = remainingParts
//        if partSize < readSize then lts.read(partPosition, partSize, resultOffset) #::: recurse(rest, readSize - partSize, resultOffset + partSize)
//        else lts.read(partPosition, readSize, resultOffset)
//
//      recurse(partsToReadFrom, readSize, readFrom).iterator

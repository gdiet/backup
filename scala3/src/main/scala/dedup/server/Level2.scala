package dedup
package server

class Level2(settings: Settings) extends AutoCloseable with util.ClassLogging:

  private val con = db.H2.connection(settings.dbDir, settings.readonly)
  private val database = db.Database(con)
  export database.{child, children, delete, mkDir, mkFile, setTime, update}

  /** id -> DataEntry. Remember to synchronize. */
  private var files = Map[Long, DataEntry]()

  override def close(): Unit =
    // TODO see original
    // TODO warn about unclosed data entries and non-empty temp dir
    // TODO delete empty temp dir
    con.close()

  /** Reads bytes from the referenced file.
    *
    * Note: The caller must make sure that no read beyond end-of-entry takes place
    * here because the behavior in that case is undefined. TODO define it.
    *
    * @param id           id of the file to read from.
    * @param dataId       dataId of the file to read from in case the file is not cached.
    * @param offset       offset in the file to start reading at.
    * @param size         number of bytes to read, NOT limited by the internal size limit for byte arrays.
    *
    * @return A contiguous LazyList(position, bytes) where data chunk size is limited to [[dedup.memChunk]].
    */
  def read(id: Long, dataId: Long, offset: Long, size: Long): LazyList[(Long, Array[Byte])] =
    synchronized(files.get(id)) match
      case None =>
        readFromLts(database.parts(dataId), offset, size)
      case Some(entry) =>
        lazy val ltsParts = database.parts(entry.baseDataId.get())
        entry.readUnsafe(offset, size).flatMap {
          case holeOffset -> Left(holeSize) => readFromLts(ltsParts, holeOffset, holeSize)
          case dataOffset -> Right(data)    => LazyList(dataOffset -> data)
        }

  /** Reads bytes from the long term store from a file defined by `parts`.
    *
    * Note: The caller must make sure that no read beyond end-of-entry takes place
    * here because the behavior in that case is undefined. TODO define it.
    *
    * @param parts    List of (offset, size) defining the parts of the file to read from.
    *                 `readFrom` + `readSize` must not exceed summed part sizes.
    * @param readFrom Position in the file to start reading at, must be >= 0.
    * @param readSize Number of bytes to read, must be >= 0.
    *
    * @return A contiguous LazyList(position, bytes) where data chunk size is limited to [[dedup.memChunk]].
    */
  private def readFromLts(parts: Seq[(Long, Long)], readFrom: Long, readSize: Long): LazyList[(Long, Array[Byte])] =
    ???
//    log.trace(s"readFromLts(parts: $parts, readFrom: $readFrom, readSize: $readSize)")
//    require(readFrom >= 0, s"Read offset $readFrom must be >= 0.")
//    require(readSize > 0, s"Read size $readSize must be > 0.")
//    val (lengthOfParts, partsToReadFrom) = parts.foldLeft(0L -> Vector[(Long, Long)]()) {
//      case ((currentOffset, result), part @ (partPosition, partSize)) =>
//        val distance = readFrom - currentOffset
//        if (distance > partSize) currentOffset + partSize -> result
//        else if (distance > 0) currentOffset + partSize -> (result :+ (partPosition + distance, partSize - distance))
//        else currentOffset + partSize -> (result :+ part)
//    }
//    require(lengthOfParts >= readFrom + readSize, s"Read offset $readFrom size $readSize exceeds parts length $parts.")
//    def recurse(remainingParts: Seq[(Long, Long)], readSize: Long, resultOffset: Long): LazyList[(Long, Array[Byte])] = {
//      val (partPosition, partSize) +: rest = remainingParts
//      if (partSize < readSize) lts.read(partPosition, partSize, resultOffset) #::: recurse(rest, readSize - partSize, resultOffset + partSize)
//      else lts.read(partPosition, readSize, resultOffset)
//    }
//    recurse(partsToReadFrom, readSize, readFrom)

end Level2

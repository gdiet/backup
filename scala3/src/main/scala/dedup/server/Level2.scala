package dedup
package server

import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.ExecutionContext

object Level2:
  def cacheLoad: Long = entriesSize.get() * entryCount.get()
  private val entryCount = new AtomicLong()
  private val entriesSize = new AtomicLong()

class Level2(settings: Settings) extends AutoCloseable with util.ClassLogging:
  import Level2._

  private val lts = store.LongTermStore(settings.dataDir, settings.readonly)
  private val con = db.H2.connection(settings.dbDir, settings.readonly)
  private val database = db.Database(con)
  export database.{child, children, delete, mkDir, mkFile, setTime, update}

  /** id -> DataEntry. Remember to synchronize. */
  private var files = Map[Long, DataEntry]()

  /** Store logic relies on this being a single thread executor. */
  private val singleThreadStoreContext = ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor())

  override def close(): Unit =
    if DataEntry.openEntries > 0 then
      log.info(s"Persisting remaining ${DataEntry.openEntries} entries, combined size ${readableBytes(entriesSize.get())} ...")
    singleThreadStoreContext.shutdown()
    singleThreadStoreContext.awaitTermination(Long.MaxValue, TimeUnit.DAYS)
    if entryCount.get > 0 then log.warn(s"${entryCount.get} entries have not been reported closed.")
    if settings.temp.exists() then
      if settings.temp.list().isEmpty then settings.temp.delete()
      else log.warn(s"Temp dir not empty: ${settings.temp}")
    lts.close()
    con.close()

  def size(id: Long, dataId: DataId): Long =
    synchronized(files.get(id)).map(_.size).getOrElse(database.dataSize(dataId))

  /** Reads bytes from the referenced file.
    *
    * @param id           id of the file to read from.
    * @param dataId       dataId of the file to read from in case the file is not cached.
    * @param offset       offset in the file to start reading at.
    * @param size         number of bytes to read, NOT limited by the internal size limit for byte arrays.
    *
    * @return A contiguous LazyList(position, bytes) where data chunk size is limited to [[dedup.memChunk]].
    * @throws IllegalArgumentException if `offset` / `size` exceed the bounds of the virtual file.
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
    * @param parts    List of (offset, size) defining the parts of the file to read from.
    *                 `readFrom` + `readSize` must not exceed summed part sizes.
    * @param readFrom Position in the file to start reading at, must be >= 0.
    * @param readSize Number of bytes to read, must be >= 0.
    *
    * @return A contiguous LazyList(position, bytes) where data chunk size is limited to [[dedup.memChunk]].
    * @throws IllegalArgumentException if `readFrom` or `readSize` exceed the bounds defined by `parts`.
    */
  private def readFromLts(parts: Seq[(Long, Long)], readFrom: Long, readSize: Long): LazyList[(Long, Array[Byte])] =
    log.trace(s"readFromLts(parts: $parts, readFrom: $readFrom, readSize: $readSize)")
    require(readFrom >= 0, s"Read offset $readFrom must be >= 0.")
    require(readSize > 0, s"Read size $readSize must be > 0.")
    val (lengthOfParts, partsToReadFrom) = parts.foldLeft(0L -> Vector[(Long, Long)]()) {
      case ((currentOffset, result), part @ (partPosition, partSize)) =>
        val distance = readFrom - currentOffset
        if distance > partSize then currentOffset + partSize -> result
        else if distance > 0 then currentOffset + partSize -> (result :+ (partPosition + distance, partSize - distance))
        else currentOffset + partSize -> (result :+ part)
    }
    require(lengthOfParts >= readFrom + readSize, s"Read offset $readFrom size $readSize exceeds parts length $parts.")
    def recurse(remainingParts: Seq[(Long, Long)], readSize: Long, resultOffset: Long): LazyList[(Long, Array[Byte])] =
      val (partPosition, partSize) +: rest = remainingParts
      if partSize < readSize then lts.read(partPosition, partSize, resultOffset) #::: recurse(rest, readSize - partSize, resultOffset + partSize)
      else lts.read(partPosition, readSize, resultOffset)
    recurse(partsToReadFrom, readSize, readFrom)

package dedup
package server

import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

object Level2:
  def cacheLoad: Long = entriesSize.get() * entryCount.get()
  private val entryCount = new AtomicLong()
  private val entriesSize = new AtomicLong()

class Level2(settings: Settings) extends AutoCloseable with util.ClassLogging:
  import Level2._

  private val lts = store.LongTermStore(settings.dataDir, settings.readonly)
  private val con = db.H2.connection(settings.dbDir, settings.readonly)
  private val database = db.Database(con)
  private val startOfFreeData = new AtomicLong(database.startOfFreeData)
  export database.{child, children, delete, mkDir, mkFile, setTime, update}

  // FIXME what happens if a tree entry is deleted and after that the level 2 cache is written?

  /** id -> DataEntry. Remember to synchronize. */
  private var files = Map[Long, DataEntry]()

  /** Store logic relies on this being a single thread executor. */
  private val singleThreadStoreContext = ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor())

  override def close(): Unit =
    // TODO is it necessary to flush files, or is this automatically done?
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

  def newDataEntry(id: Long, baseDataId: DataId): DataEntry =
    synchronized(files.get(id)) match
      case None => DataEntry(AtomicLong(baseDataId.toLong), database.dataSize(baseDataId), settings.tempPath)
      case Some(entry) => DataEntry(entry.baseDataId, entry.size, settings.tempPath)

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
  def read(id: Long, dataId: DataId, offset: Long, size: Long): LazyList[(Long, Array[Byte])] =
    synchronized(files.get(id)) match
      case None =>
        readFromLts(database.parts(dataId), offset, size)
      case Some(entry) =>
        lazy val ltsParts = database.parts(entry.getBaseDataId)
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

  /* Note: Once in Level2, DataEntry objects are never mutated. */
  def persist(id: Long, dataEntry: DataEntry): Unit =
    // First wait that any previous persist request for this file is finished.
    synchronized(files.get(id)).foreach(_.awaitClosed())
    if dataEntry.size == 0 then
      // If data entry size is zero, explicitly set dataId -1 because it might have contained something else...
      database.setDataId(id, DataId(-1))
      dataEntry.close(DataId(-1))
    else
      log.trace(s"ID $id - persisting data entry, size ${dataEntry.size} / base data id ${dataEntry.baseDataId}.")
      entryCount.incrementAndGet()
      entriesSize.addAndGet(dataEntry.size)
      // Persist async. Creating the future synchronized makes sure data entries are processed in the right order.
      synchronized {
        files += id -> dataEntry
        Future(persistAsync(id, dataEntry))(singleThreadStoreContext)
      }

  private def persistAsync(id: Long, dataEntry: DataEntry): Unit = try {
    val ltsParts = database.parts(dataEntry.getBaseDataId)
    def data: LazyList[(Long, Array[Byte])] = dataEntry.readUnsafe(0, dataEntry.size).flatMap {
      case position -> Right(data) => LazyList(position -> data)
      case position -> Left(offset) => readFromLts(ltsParts, position, offset)
    }
    // Calculate hash
    val md = java.security.MessageDigest.getInstance(hashAlgorithm)
    data.foreach(entry => md.update(entry._2))
    val hash = md.digest()
    // Check if already known
    val dataId = database.dataEntry(hash, dataEntry.size) match {
      // Already known, simply link
      case Some(dataId) =>
        database.setDataId(id, dataId)
        log.trace(s"Persisted $id - content known, linking to dataId $dataId")
        dataId
      // Not yet known, store ...
      case None =>
        // Reserve storage space
        val start = startOfFreeData.getAndAdd(dataEntry.size)
        // Write to storage
        data.foreach { case (offset, bytes) => lts.write(start + offset, bytes) }
        // create data entry
        val dataId = database.newDataIdFor(id)
        database.insertDataEntry(dataId, 1, dataEntry.size, start, start + dataEntry.size, hash)
        log.trace(s"Persisted $id - new content, dataId $dataId")
        dataId
    }
    // Release persisted DataEntry.
    synchronized {
      files -= id
      entryCount.decrementAndGet()
      entriesSize.addAndGet(-dataEntry.size)
      dataEntry.close(dataId)
    }
  } catch (e: Throwable) => { log.error(s"Persisting $id failed.", e); throw e }

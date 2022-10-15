package dedup
package server

import dedup.db.withStatement

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Executors, TimeUnit}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Using.resource

object Level2:
  def cacheLoad: Long = entriesSize.get() * entryCount.get()
  private val entryCount = new AtomicLong()
  private val entriesSize = new AtomicLong()

  def writeAlgorithm(data: Iterator[(Long, Array[Byte])], toAreas: Seq[DataArea], write: (Long, Array[Byte]) => Unit): Unit =
    @annotation.tailrec
    def doStore(areas: Seq[DataArea], data: Array[Byte]): Seq[DataArea] =
      ensure("write.algorithm.1", areas.nonEmpty, s"Remaining data areas are empty, data size ${data.length}")
      val head +: rest = areas : @unchecked // TODO Can we prove that areas is nonempty? Do we need the ensure above?
      if head.size == data.length then
        write(head.start, data); rest
      else if head.size > data.length then
        write(head.start, data); head.drop(data.length) +: rest
      else
        val intSize = head.size.toInt // always smaller than MaxInt, see above
        write(head.start, data.take(intSize)); doStore(rest, data.drop(intSize))
    val remaining = data.foldLeft(toAreas) { case (storeAt, (_, bytes)) => doStore(storeAt, bytes) }
    ensure("write.algorithm.2", remaining.isEmpty, s"Remaining data areas not empty: $remaining")

/* Corner case: What happens if a tree entry is deleted and after that the level 2 cache is written?
 * In that case, level 2 cache is written for the deleted file entry, and everything is fine. */
class Level2(settings: Settings) extends AutoCloseable with util.ClassLogging:
  import Level2.*

  private val lts = store.LongTermStore(settings.dataDir, settings.readonly)
  private val con = db.H2.connection(settings.dbDir, settings.readonly)
  private val database = db.Database(con)
  private val freeAreas = if settings.readonly then FreeAreas() else FreeAreas(database.freeAreas())
  export database.{child, children, deleteChildless, entry, mkDir, mkFile, setTime, split, update}

  /** id -> DataEntry. Remember to synchronize. */
  private var files = Map[Long, DataEntry]()

  /** Store logic relies on this being a single thread executor. */
  private val singleThreadStoreContext = ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor())

  override def close(): Unit =
    if DataEntry.openEntries > 0 then
      log.info(s"Closing remaining ${DataEntry.openEntries} entries, combined size ${readableBytes(entriesSize.get())} ...")
    singleThreadStoreContext.shutdown()
    singleThreadStoreContext.awaitTermination(Long.MaxValue, TimeUnit.DAYS) // This flushes all pending files.
    if entryCount.get > 0 then log.warn(s"${entryCount.get} entries have not been reported closed.")
    if settings.temp.exists() then
      if settings.temp.list().isEmpty then settings.temp.delete()
      else log.warn(s"Temp dir not empty: ${settings.temp}")
    lts.close()
    if !settings.readonly then database.shutdownCompact()
    con.close()
    log.info("Shutdown complete.")

  def newDataEntry(id: Long, baseDataId: DataId): DataEntry =
    synchronized(files.get(id)) match
      case None => DataEntry(AtomicLong(baseDataId.asLong), database.dataSize(baseDataId), settings.tempPath)
      case Some(entry) => DataEntry(entry.baseDataId, entry.size, settings.tempPath)

  def size(file: FileEntry): Long =
    synchronized(files.get(file.id)).map(_.size).getOrElse(database.dataSize(file.dataId))

  /** Reads bytes from the referenced file.
    *
    * @param id           id of the file to read from.
    * @param dataId       dataId of the file to read from in case the file is not cached.
    * @param offset       offset in the file to start reading at.
    * @param size         number of bytes to read, NOT limited by the internal size limit for byte arrays.
    *
    * @return A contiguous Iterator(position, bytes) where data chunk size is limited to [[dedup.memChunk]].
    * @throws IllegalArgumentException if `offset` / `size` exceed the bounds of the virtual file.
    */
  def read(id: Long, dataId: DataId, offset: Long, size: Long): Iterator[(Long, Array[Byte])] =
    synchronized(files.get(id)) match
      case None =>
        readFromLts(database.parts(dataId), offset, size)
      case Some(entry) =>
        lazy val ltsParts = database.parts(entry.getBaseDataId)
        entry.readUnsafe(offset, size).flatMap {
          case holeOffset -> Left(holeSize) => readFromLts(ltsParts, holeOffset, holeSize)
          case dataOffset -> Right(data)    => Iterator(dataOffset -> data)
        }

  /** From the long term store, reads file content defined by `parts`.
    *
    * @param parts    List of (offset, size) defining the file content parts to read.
    *                 `readFrom` + `readSize` should not exceed summed part sizes unless
    *                 `parts` is the empty list that is used for blacklisted entries.
    * @param readFrom Position in the file to start reading at, must be >= 0.
    * @param readSize Number of bytes to read, must be >= 0.
    *
    * @return A contiguous Iterator(position, bytes) where data chunk size is limited to [[dedup.memChunk]].
    *         If `parts` is too small, the data is filled up with zeros.
    * @throws IllegalArgumentException if `readFrom` is negative or `readSize` is less than 1.
    */
  private def readFromLts(parts: Seq[(Long, Long)], readFrom: Long, readSize: Long): Iterator[(Long, Array[Byte])] =
    log.trace(s"readFromLts(readFrom: $readFrom, readSize: $readSize, parts: $parts)")
    ensure("read.lts.offset", readFrom >= 0, s"Read offset $readFrom must be >= 0.")
    ensure("read.lts.size", readSize > 0, s"Read size $readSize must be > 0.")
    val partsToReadFrom = parts.foldLeft(0L -> Vector[(Long, Long)]()) {
      case ((currentOffset, result), part @ (partPosition, partSize)) =>
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
    def data: Iterator[(Long, Array[Byte])] = dataEntry.readUnsafe(0, dataEntry.size).flatMap {
      case position -> Right(data) => Iterator(position -> data)
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
        val reserved = freeAreas.reserve(dataEntry.size)
        // Write to storage
        Level2.writeAlgorithm(data, reserved, lts.write)
        // Save data entries
        val dataId = database.newDataIdFor(id)
        reserved.zipWithIndex.foreach { case (dataArea, index) =>
          log.debug(s"Data ID $dataId size ${dataEntry.size} - persisted at ${dataArea.start} size ${dataArea.size}")
          database.insertDataEntry(dataId, index + 1, dataEntry.size, dataArea.start, dataArea.stop, hash)
        }
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
  } catch (e: Throwable) => { log.error(s"Persisting $id failed: $dataEntry", e); throw e }

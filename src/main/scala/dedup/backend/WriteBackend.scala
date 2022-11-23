package dedup
package backend

import dedup.db.WriteDatabase
import dedup.server.Settings

import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.ExecutionContext

/** Don't instantiate more than one backend for a repository. */
// Why not? Because the backend object is used for synchronization.
final class WriteBackend(settings: Settings, db: WriteDatabase) extends ReadBackend(settings, db):

  private val freeAreas = server.FreeAreas(db.freeAreas())
  private val files = FileHandlesWrite(settings.tempPath)
  /** Store logic relies on this being a single thread executor. */
  private val singleThreadStoreContext = ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor())

  override def shutdown(): Unit = sync {
    // Stop creating new write handles and process existing write handles.
    files.shutdown().foreach { case fileId -> maybeDataEntry =>
      releaseInternal(fileId) match // Get read handle for the file.
        case None => if maybeDataEntry.isDefined
          then log.error(s"No read handle for file $fileId although new data was written - data loss!")
          else log.warn(s"No read handle for file $fileId although write handle was present.")
        case Some(count -> dataId) =>
          for _ <- 1 to count do releaseInternal(fileId) // Fully release read handle.
          maybeDataEntry.foreach(enqueue(fileId, dataId, _)) // If necessary, enqueue open data entry.
    }
    // Finish pending writes.
    singleThreadStoreContext.shutdown()
    singleThreadStoreContext.awaitTermination(Long.MaxValue, TimeUnit.DAYS)
    // Clean up.
    db.shutdownCompact()
    super.shutdown()
  }

  override def size(fileEntry: FileEntry): Long =
    files.getSize(fileEntry.id).getOrElse(super.size(fileEntry))

  override def mkDir(parentId: Long, name: String): Option[Long] =
    db.mkDir(parentId, name)

  override def setTime(id: Long, newTime: Long): Unit =
    db.setTime(id, newTime)

  override def deleteChildless(entry: TreeEntry): Boolean =
    db.deleteChildless(entry.id)

  override def open(fileId: Long, dataId: DataId): Unit =
    super.open(fileId, dataId)
    files.addIfMissing(fileId)

  override def createAndOpen(parentId: Long, name: String, time: Time): Option[Long] =
    // A sensible Option.tapEach might be available in future Scala versions, see
    // https://stackoverflow.com/questions/67017901/why-does-scala-option-tapeach-return-iterable-not-option
    // and https://github.com/scala/scala-library-next/pull/80
    db.mkFile(parentId, name, time, DataId(-1)).tap(_.foreach { fileId =>
      super.open(fileId, DataId(-1))
      if !files.addIfMissing(fileId) then log.error(s"File handle (write) was already present for file $fileId.")
    })

  override def copyFile(file: FileEntry, newParentId: Long, newName: String): Boolean =
    db.mkFile(newParentId, newName, file.time, file.dataId).isDefined

  private def dataEntry(fileId: Long): Option[DataEntry] =
    files.dataEntry(fileId, dataId.andThen(db.logicalSize))

  override def write(fileId: Long, data: Iterator[(Long, Array[Byte])]): Boolean =
    dataEntry(fileId).map(_.write(data)).isDefined

  override def truncate(fileId: Long, newSize: Long): Boolean =
    dataEntry(fileId).map(_.truncate(newSize)).isDefined

  override def read(fileId: Long, offset: Long, requestedSize: Long): Option[Iterator[(Long, Array[Byte])]] =
    files.read(fileId, offset, requestedSize).map(_.flatMap {
      case (position, Left(holeSize)) => super.read(fileId, position, holeSize).getOrElse {
        log.error(s"Reading base layer of file $fileId failed at $position + $holeSize, inserting zeros. This is a bug.")
        Iterator.range(position, position + holeSize, memChunk.toLong).map { localPos =>
          localPos -> new Array[Byte](cache.asInt(math.min(position + holeSize - localPos, memChunk)))
        }
      }
      case (position, Right(bytes)) => Iterator(position -> bytes)
    })

  override def release(fileId: Long): Boolean =
    log.info(s"release write - $fileId") // FIXME trace
    releaseInternal(fileId) match
      case None => false // warning is logged in releaseInternal
      case Some(count -> _) if count > 0 => true // still some handles open for the file
      case Some(_ -> dataId) => files.release(fileId).foreach(enqueue(fileId, dataId, _)); true

  private def enqueue(fileId: Long, dataId: DataId, dataEntry: DataEntry): Unit =
    WriteBackend.entryCount.incrementAndGet()
    WriteBackend.entriesSize.addAndGet(dataEntry.size)
    log.trace(s"Write cache load: ${WriteBackend.entryCount}*${WriteBackend.entriesSize}")

    singleThreadStoreContext.execute(() => try {

      def removeAndQueueNext(newDataId: DataId): Unit =
        files.removeAndGetNext(fileId, dataEntry).foreach(enqueue(fileId, newDataId, _))
        dataEntry.close()
        WriteBackend.entryCount.decrementAndGet()
        WriteBackend.entriesSize.addAndGet(-dataEntry.size)

      log.trace(s"Write through file $fileId / data ID $dataId / size ${dataEntry.size}.")

      if dataEntry.size == 0 then
        // If data entry size is zero, explicitly set dataId -1 because it might have contained something else.
        db.setDataId(fileId, DataId(-1))
        removeAndQueueNext(DataId(-1))

      else
        log.trace (s"File $fileId - persisting data entry / size ${dataEntry.size} / base data id $dataId.")
        val ltsParts = db.parts(dataId)
        def data: Iterator[(Long, Array[Byte])] = dataEntry.read(0, dataEntry.size).flatMap {
          case position -> Right(data) => Iterator(position -> data)
          case position -> Left(offset) => readFromLts(ltsParts, position, offset)
        }
        // Calculate hash
        val md = java.security.MessageDigest.getInstance(hashAlgorithm)
        data.foreach(entry => md.update(entry._2))
        val hash = md.digest()
        // Check if already known
        db.dataEntry(hash, dataEntry.size) match
          // Already known, simply link
          case Some(dataId) =>
            db.setDataId(fileId, dataId)
            log.trace(s"Persisted $fileId - content known, linking to dataId $dataId")
            removeAndQueueNext(dataId)
          // Not yet known, store ...
          case None =>
            // Reserve storage space
            val reserved = freeAreas.reserve(dataEntry.size)
            // Write to storage
            WriteBackend.writeAlgorithm(data, reserved, lts.write)
            // Save data entries
            val dataId = db.newDataIdFor(fileId)
            reserved.zipWithIndex.foreach { case (dataArea, index) =>
              log.debug(s"Data ID $dataId size ${dataEntry.size} - persisted at ${dataArea.start} size ${dataArea.size}")
              db.insertDataEntry(dataId, index + 1, dataEntry.size, dataArea.start, dataArea.stop, hash)
            }
            log.trace(s"Persisted $fileId - new content, dataId $dataId")
            removeAndQueueNext(dataId)
    } catch (e: Throwable) => { log.error(s"Persisting $fileId failed: $dataEntry", e); throw e })

object WriteBackend:
  def cacheLoad: Long = entriesSize.get() * entryCount.get()
  private val entryCount = new AtomicLong()
  private val entriesSize = new AtomicLong()

  def writeAlgorithm(data: Iterator[(Long, Array[Byte])], toAreas: Seq[DataArea], write: (Long, Array[Byte]) => Unit): Unit =
    @annotation.tailrec
    def doStore(areas: Seq[DataArea], data: Array[Byte]): Seq[DataArea] =
      ensure("write.algorithm.1", areas.nonEmpty, s"Remaining data areas are empty, data size ${data.length}")
      val head +: rest = areas: @unchecked // TODO Can we prove that areas is nonempty? Do we need the ensure above?
      if head.size == data.length then { write(head.start, data); rest }
      else if head.size > data.length then { write(head.start, data); head.drop(data.length) +: rest }
      else
        val intSize = head.size.toInt // always smaller than MaxInt, see above "if head.size > data.length"
        write(head.start, data.take(intSize))
        doStore(rest, data.drop(intSize))

    val remaining = data.foldLeft(toAreas) { case (storeAt, (_, bytes)) => doStore(storeAt, bytes) }
    ensure("write.algorithm.2", remaining.isEmpty, s"Remaining data areas not empty: $remaining")

package dedup

import dedup.store.LongTermStore

import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Executors, TimeUnit}
import scala.concurrent.{ExecutionContext, Future}

object Level2 {
  def cacheLoad: Long = _entriesSize.get() * _entryCount.get()
  private val _entryCount = new AtomicLong()
  private val _entriesSize = new AtomicLong()
}

class Level2(settings: Settings) extends AutoCloseable with ClassLogging {
  import Level2._

  private val con = H2.file(settings.dbDir, settings.readonly)
  private val db = new Database(con)
  private val lts = new LongTermStore(settings.dataDir, settings.readonly)
  private val startOfFreeData = new AtomicLong(db.startOfFreeData)
  /** Store logic relies on this being a single thread executor. */
  private val singleThreadStoreContext = ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor())

  /** id -> dataEntries. dataEntries is non-empty. Remember to synchronize. */
  private var files = Map[Long, Vector[DataEntry]]()

  override def close(): Unit = {
    if (DataEntry.openEntries > 0) log.info(s"Entries remaining to persist: ${DataEntry.openEntries}")
    singleThreadStoreContext.shutdown()
    singleThreadStoreContext.awaitTermination(Long.MaxValue, TimeUnit.DAYS)
    con.close()
  }

  def setTime(id: Long, time: Long): Unit = db.setTime(id, time)
  def dataId(id: Long): Option[Long] = db.dataId(id)
  def child(parentId: Long, name: String): Option[TreeEntry] = db.child(parentId, name)
  def children(parentId: Long): Seq[TreeEntry] = db.children(parentId)
  def delete(id: Long): Unit = db.delete(id)
  def mkDir(parentId: Long, name: String): Option[Long] = db.mkDir(parentId, name)
  def mkFile(parentId: Long, name: String, time: Long, dataId: Long): Boolean = db.mkFile(parentId, name, time, dataId)
  /** Creates a new file with dataId -1. */
  def mkFile(parentId: Long, name: String, time: Long): Option[Long] = db.mkFile(parentId, name, time)
  def update(id: Long, newParentId: Long, newName: String): Unit = db.update(id, newParentId, newName)
  def nextDataId: Long = db.nextId

  /** In Level2, DataEntry objects are not mutated. */
  def persist(id: Long, dataEntry: DataEntry): Unit =
    // If data entry size is zero, explicitly set dataId -1 because it might contained something else...
    if (dataEntry.size == 0) { db.setDataId(id, -1); dataEntry.close() }
    else {
      log.trace(s"ID $id - persisting data entry, size ${dataEntry.size} / base data id ${dataEntry.baseDataId}.")
      _entryCount.incrementAndGet()
      _entriesSize.addAndGet(dataEntry.size)
      synchronized {
        files += id -> (dataEntry +: files.getOrElse(id, Vector()))

        // Persist async. Creating the future synchronized makes sure data entries are processed in the right order.
        Future(try {
          // Here in the Future the context is not synchronized anymore.
          val ltsParts = db.parts(dataEntry.baseDataId)
          def data: LazyList[(Long, Array[Byte])] = dataEntry.readUnsafe(0, dataEntry.size)._2.flatMap {
            case Right(data) => LazyList(data)
            case Left((position, offset)) => readFromLts(ltsParts, position, offset, position)
          }
          // Calculate hash
          val md = MessageDigest.getInstance(hashAlgorithm)
          data.foreach(data => md.update(data._2))
          val hash = md.digest()
          // Check if already known
          db.dataEntry(hash, dataEntry.size) match {
            // Already known, simply link
            case Some(dataId) =>
              db.setDataId(id, dataId)
              log.trace(s"Persisted $id - content known, linking to dataId $dataId")
            // Not yet known, store ...
            case None =>
              // Reserve storage space
              val start = startOfFreeData.getAndAdd(dataEntry.size)
              // Write to storage
              data.foreach { case (offset, bytes) => lts.write(start + offset, bytes) }
              // create data entry
              val dataId = db.newDataIdFor(id)
              db.insertDataEntry(dataId, 1, dataEntry.size, start, start + dataEntry.size, hash)
              log.trace(s"Persisted $id - new content, dataId $dataId")
          }
          // Remove persisted DataEntry from level 2.
          synchronized {
            val filteredEntries = files(id).filterNot(_.id == dataEntry.id)
            if (filteredEntries.nonEmpty) files += id -> filteredEntries else {
              files -= id
              log.trace(s"Fully persisted file $id.")
            }
            _entryCount.decrementAndGet()
            _entriesSize.addAndGet(-dataEntry.size)
            dataEntry.close()
          }
        } catch { case e: Throwable => log.error(s"Persisting $id failed.", e); throw e })(singleThreadStoreContext)
      }
    }

  def size(id: Long, dataId: Long): Long =
    synchronized(files.get(id)).map(_.head.size).getOrElse(db.dataSize(dataId))

  /** @param parts    List of (position, size) defining the LTS parts of the file to read from.
    *                 `readFrom` + `readSize` must not exceed summed part sizes.
    * @param readFrom Position in the file to start reading at, must be >= 0.
    * @param readSize Number of bytes to read, must be >= 0.
    * @return A contiguous LazyList(position, bytes) where data chunk size is limited to [[dedup.memChunk]]. */
  private def readFromLts(parts: Seq[(Long, Long)], readFrom: Long, readSize: Long, resultOffset: Long): LazyList[(Long, Array[Byte])] = {
    require(readFrom >= 0, s"Read offset $readFrom must be >= 0.")
    require(readSize > 0, s"Read size $readSize must be > 0.")
    val (lengthOfParts, partsToReadFrom) = parts.foldLeft(0L -> Vector[(Long, Long)]()) {
      case ((currentOffset, result), part @ (partPosition, partSize)) =>
        val distance = readFrom - currentOffset
        if (distance > partSize) currentOffset + partSize -> result
        else if (distance > 0) currentOffset + partSize -> (result :+ (partPosition + distance, partSize - distance))
        else currentOffset + partSize -> (result :+ part)
    }
    require(lengthOfParts >= readFrom + readSize, s"Read offset $readFrom size $readSize exceeds parts length $parts.")
    def recurse(remainingParts: Seq[(Long, Long)], readSize: Long, resultOffset: Long): LazyList[(Long, Array[Byte])] = {
      val (partPosition, partSize) +: rest = remainingParts
      if (partSize < readSize) lts.read(partPosition, partSize, resultOffset) #::: recurse(rest, readSize - partSize, resultOffset + partSize)
      else lts.read(partPosition, readSize, resultOffset)
    }
    recurse(partsToReadFrom, readSize, 0L)
  }

  /** Implementation (hopefully) guarantees that no read beyond end-of-entry takes place here. */
  def read[D: DataSink](id: Long, dataId: Long, offset: Long, size: Long, sink: D): Unit = {
    lazy val ltsParts = db.parts(dataId)
    @annotation.tailrec
    def readFrom(entries: Vector[DataEntry], holes: Vector[(Long, Long)]): Unit = {
      entries match {
        case Vector() =>
          holes.foreach { case (position, size) =>
            readFromLts(ltsParts, position, size, position).foreach { case partPos -> data =>
              sink.write(partPos, data)
            }
          }
        case entry +: remaining =>
          val newHoles = holes.flatMap { case (position, size) => entry.read(position, size, sink)._2 }
          if (newHoles.nonEmpty) readFrom(remaining, newHoles)
      }
    }
    readFrom(synchronized(files.get(id)).getOrElse(Vector()), Vector(offset -> size))
  }
}

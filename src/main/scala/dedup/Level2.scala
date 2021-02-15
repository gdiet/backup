package dedup

import dedup.store.LongTermStore

import java.security.MessageDigest
import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.{ExecutionContext, Future}

class Level2(settings: Settings) extends AutoCloseable with ClassLogging {
  private val con = H2.file(settings.dbDir, settings.readonly)
  private val db = new Database(con)
  private val lts = new LongTermStore(settings.dataDir, settings.readonly)
  private val startOfFreeData = new AtomicLong(db.startOfFreeData)
  private val storeContext = ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor())

  /** id -> dataEntries. dataEntries is non-empty. Remember to synchronize. */
  private var files = Map[Long, Vector[DataEntry]]()

  override def close(): Unit = {
    storeContext.shutdown()
    storeContext.awaitTermination(Long.MaxValue, TimeUnit.DAYS)
    con.close()
  }

  def setTime(id: Long, time: Long): Unit = db.setTime(id, time)
  def dataId(id: Long): Option[Long] = db.dataId(id)
  def child(parentId: Long, name: String): Option[TreeEntry] = db.child(parentId, name)
  def children(parentId: Long): Seq[TreeEntry] = db.children(parentId)
  def delete(id: Long): Unit = db.delete(id)
  def mkDir(parentId: Long, name: String): Long = db.mkDir(parentId, name)
  def mkFile(parentId: Long, name: String, time: Long, dataId: Long): Unit = db.mkFile(parentId, name, time, dataId)
  /** Creates a new file with dataId -1. */
  def mkFile(parentId: Long, name: String, time: Long): Long = db.mkFile(parentId, name, time)
  def update(id: Long, newParentId: Long, newName: String): Unit = db.update(id, newParentId, newName)
  def nextDataId: Long = db.nextId

  /** In Level2, DataEntry objects are not mutated. */
  def persist(id: Long, dataEntry: DataEntry): Unit =
    // If data entry size is zero, explicitly set dataId -1 because it might have been set to something else before...
    if (dataEntry.size == 0) { db.setDataId(id, -1); dataEntry.close() }
    else {
      trace_(s"ID $id - persisting data entry, size ${dataEntry.size} / base data id ${dataEntry.baseDataId}.")
      synchronized(files += id -> (dataEntry +: files.getOrElse(id, Vector())))

      // Persist async
      Future {
        val end = dataEntry.size
        // Must be def to avoid memory problems.
        def data = LazyList.range(0L, end, memChunk).flatMap { position =>
          val chunkSize = math.min(memChunk, end - position).toInt
          dataEntry.read(position, chunkSize, readFromLts)
        }
        // Calculate hash
        val md = MessageDigest.getInstance(hashAlgorithm)
        data.foreach(md.update)
        val hash = md.digest()
        // Check if already known
        db.dataEntry(hash, dataEntry.size) match {
          // Already known, simply link
          case Some(dataId) =>
            trace_(s"Persisted $id - content known, linking to dataId $dataId")
            db.setDataId(id, dataId)
          // Not yet known, store ...
          case None =>
            // Reserve storage space
            val start = startOfFreeData.getAndAdd(dataEntry.size)
            // Write to storage
            data.foldLeft(0L) { case (position, data) =>
              lts.write(start + position, data)
              position + data.length
            }
            // 5c. create data entry
            val dataId = db.newDataIdFor(id)
            db.insertDataEntry(dataId, 1, dataEntry.size, start, start + dataEntry.size, hash)
            trace_(s"Persisted $id - new content, dataId $dataId")
        }
        synchronized(files -= id)
        dataEntry.close()
      }(storeContext)
    }

  def size(id: Long, dataId: Long): Long =
    synchronized(files.get(id)).map(_.head.size).getOrElse(db.dataSize(dataId))

  private def readFromLts(dataId: Long, readFrom: Long, readSize: Int): Data = {
    require(readSize > 0, s"Read size $readSize !> 0")
    val readEnd = readFrom + readSize
    val endPosition -> data = db.parts(dataId).foldLeft(0L -> Vector.empty[Array[Byte]]) { case (readPosition -> result, chunkStart -> chunkEnd) =>
      val chunkLen = chunkEnd - chunkStart
      if (readPosition + chunkLen <= readFrom) readPosition + chunkLen -> result
      else  if (readPosition >= readEnd) readPosition -> result
      else {
        val skipInChunk = math.max(0, readFrom - readPosition)
        val takeOfChunk = math.min(chunkLen - skipInChunk, readEnd - readPosition - skipInChunk).toInt // TODO ensure this is always Int
        readPosition + skipInChunk + takeOfChunk -> (result :+ lts.read(chunkStart + skipInChunk, takeOfChunk))
      }
    }
    require(endPosition <= readEnd, s"Actually read $endPosition !<= read end $readEnd")
    if (endPosition >= readEnd) data else data :+ new Array((readSize - endPosition).toInt)
  }

  def read(id: Long, dataId: Long, offset: Long, size: Int): Data =
    synchronized(files.get(id))
      .map { entries =>
        entries.reverse.foldLeft(readFromLts _) { case (readMethod, entry) =>
          (_, off, siz) => entry.read(off, siz, readMethod)
        }(dataId, offset, size)
      }
      .getOrElse(readFromLts(dataId, offset, size))
}

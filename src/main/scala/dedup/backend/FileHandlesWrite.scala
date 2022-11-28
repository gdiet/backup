package dedup
package backend

import dedup.util.ClassLogging

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicLong

/** The [[WriteBackend]] needs to keep track of cached writes to open files. For this, when a file is opened (for
  * reading or writing), a [[Placeholder]] entry is created. On demand - i.e. for write operations -, the placeholder
  * is replaced by a [[DataEntry]] that caches writes. Creating the data entry on demand only is implemented because
  * creating it needs two database reads which can be spared for read-only access to files. When the last open handle
  * for the file is released, the data entry is queued for persisting, and the "current" part of the entry is set to
  * [[Empty]]. A queue is used because persisting might take some time, and in the meanwhile another open-write-release
  * cycle may have happened that - to avoid conflicts - must not be persisted before the previous persist process is
  * finished. When the last data entry for a file has been persisted, the write handle is removed if the "current" part
  * is still empty.
  *
  * A single instance of this class is used by the [[WriteBackend]] to manage the write file handles. */
class FileHandlesWrite(tempPath: Path) extends ClassLogging:
  private object Empty
  private object Placeholder
  private type Current = Empty.type | Placeholder.type | DataEntry

  /** file id -> (current, storing). Remember to synchronize. */
  private var files = Map[Long, (Current, Seq[DataEntry])]()
  private var closing = false
  private val dataSeq = AtomicLong()

  /** Stop creating new data entries. [[release]] all "current" data entries.
    * @return The results of [[release]]. For all of them, the read handles need to be released, and if a [[DataEntry]]
    *         is provided, it must be enqueued for persisting. */
  def shutdown(): Map[Long, Option[DataEntry]] = synchronized {
    log.debug(s"Shutting down - ${files.size} write handles present.")
    closing = true
    // FIXME same thing here - do we need to persist 0 bytes for Placeholder entries?
    val openWriteHandles = files.collect { case fileId -> (_: DataEntry, _) => fileId -> release(fileId) }
    if openWriteHandles.nonEmpty then
      log.warn(s"Still ${openWriteHandles.size} open write handles when unmounting the file system.")
    openWriteHandles
  }

  /** @return The size of the file handle, [[None]] if handle is missing or empty. */
  def getSize(fileId: Long): Option[Long] =
    synchronized(files.get(fileId)).flatMap {
      case (current: DataEntry) -> _ => Some(current.size)
      case _ -> (head +: _)          => Some(head.size)
      case _ -> _                    => None
    }

  /** Create and add placeholder handle if missing.
    * @return `true` if added, `false` if already present. */
  def addIfMissing(fileId: Long): Boolean = synchronized {
    log.info(s"addIfMissing $fileId - $files") // FIXME trace
    files.get(fileId) match
      case None => files += fileId -> (Placeholder, Seq()); true
      case Some(Empty -> storing) => files += fileId -> (Placeholder, storing); true
      case _ => false
  }

  /** @return The data entry (created if missing) of the handle, [[None]] if handle is missing. */
  def dataEntry(fileId: Long, sizeInDb: Long => Long): Option[DataEntry] = synchronized {
    if closing then None else files.get(fileId) match
      case Some((current: DataEntry) -> _) => Some(current)
      case Some(Placeholder -> storing) =>
        log.trace(s"Creating write cache for $fileId.")
        val initialSize = storing.headOption.map(_.size).getOrElse(sizeInDb(fileId))
        Some(DataEntry(dataSeq, initialSize, tempPath).tap(entry => files += fileId -> (entry, storing)))
      case _ =>
        log.info(s"dataEntry for $fileId not present in files $files") // TODO trace
        None
  }

  /** @return [[None]] if handle is missing. */
  def read(fileId: Long, offset: Long, requestedSize: Long): Option[Iterator[(Long, Either[Long, Array[Byte]])]] =
    println(s"read files: $files") // TODO
    synchronized(files.get(fileId)) match
      case Some(current -> storing) =>
        (current match
          case dataEntry: DataEntry => Some(dataEntry.read(offset, requestedSize))
          case _ => Some(Iterator(offset -> Left(requestedSize)))
        ).map(_.flatMap {
          // fill in holes in data by recursing through the storing sequence
          case (position, Left(holeSize)) => fillHoles(position, holeSize, storing)
          case data => Iterator(data)
        })
      case None => log.info(s"read $fileId - not in files $files"); None // TODO trace

  private def fillHoles(position: Long, holeSize: Long, remaining: Seq[DataEntry]): Iterator[(Long, Either[Long, Array[Byte]])] =
    remaining match
      case Seq() => Iterator(position -> Left(holeSize))
      case head +: tail =>
        head.read(position, holeSize).flatMap {
          case (innerPosition, Left(innerHoleSize)) => fillHoles(innerPosition, innerHoleSize, tail)
          case other => Iterator(other)
        }

  /** Move the current data entry to the storing queue, returning it if the storing queue was empty before.
    * @return The data entry if the storing queue was empty, so the data entry needs to be enqueued immediately. */
  def release(fileId: Long): Option[DataEntry] = synchronized {
    log.info(s"release handles - $fileId - $files") // FIXME trace
    files.get(fileId) match
      case Some((current: DataEntry) -> storing) =>
        files += fileId -> (Empty, current +: storing)
        if storing.isEmpty then Some(current) else None
      // FIXME if a placeholder is there, are we supposed to persist a 0 length entry?
      case Some(Placeholder -> storing) => files += fileId -> (Empty, storing); None
      case other =>
        log.info(s"release: Missing entry for $fileId ... $other") // FIXME trace
        problem("WriteHandle.release", s"Missing file handle (write) for file $fileId.")
        None
  }

  def removeAndGetNext(fileId: Long, dataEntry: DataEntry): Option[DataEntry] = synchronized {
    log.info(s"removeAndGetNext($fileId, dataEntry $dataEntry) - $files") // TODO trace
    files.get(fileId) match
      case None =>
        problem("WriteHandle.removeAndGetNext.missing", s"Missing file handle (write) for file $fileId.")
        None
      case Some(Empty -> Seq(`dataEntry`)) => files -= fileId; None
      case Some((current, others :+ `dataEntry`)) => files += fileId -> (current, others); others.lastOption
      case Some((_, others)) =>
        problem("WriteHandle.removeAndGetNext.mismatch", s"Previous DataEntry not found for file $fileId.")
        others.lastOption
  }

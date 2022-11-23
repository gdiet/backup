package dedup
package backend

import dedup.util.ClassLogging

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicLong

/** As long as a file is open, the [[WriteBackend]] caches all writes to that file in a [[DataEntry]]. [[DataEntry]]
  * instances are created on demand only, because they need two database reads - for read-only access to files, those
  * two reads can be spared. This means that the "current" part of a write file handle is an [[Option]]([[DataEntry]]).
  * When the last handle to a file is released, any written data (i.e., if a "current" [[DataEntry]] is present) needs
  * to be queued for persisting. This is a queue, because persisting might take some time, and in the meanwhile another
  * open-write-release cycle may have happened that - to avoid conflicts - must not be persisted before the previous
  * is persist process finished.
  *
  * A single instance of this class is used by the [[WriteBackend]] to manage the write file handles. */
class FileHandlesWrite(tempPath: Path) extends ClassLogging:
  /** file id -> (current, storing). Remember to synchronize. */
  private var files = Map[Long, (Option[DataEntry], Seq[DataEntry])]()
  private var closing = false
  private val dataSeq = AtomicLong()

  /** Stop creating new data entries. [[release]] all "current" data entries.
    * @return The results of [[release]]. For all of them, the read handles need to be released, and if a [[DataEntry]]
    *         is provided, it must be enqueued for persisting. */
  def shutdown(): Map[Long, Option[DataEntry]] = synchronized {
    log.info(s"shutdown - $files") // FIXME debug
    closing = true
    val openWriteHandles = files.collect { case (fileId, Some(_) -> _) => fileId -> release(fileId) }
    if openWriteHandles.nonEmpty then
      log.warn(s"Still ${openWriteHandles.size} open write handles when unmounting the file system.")
    openWriteHandles
  }

  /** @return The size of the file handle, [[None]] if handle is missing or empty. */
  def getSize(fileId: Long): Option[Long] =
    synchronized(files.get(fileId)).flatMap {
      case Some(current) -> _  => Some(current.size)
      case None -> (head +: _) => Some(head.size)
      case None ->          _  => None
    }

  /** Create and add empty handle if missing.
    * @return `true` if added, `false` if already present. */
  def addIfMissing(fileId: Long): Boolean = synchronized {
    (!files.contains(fileId)).tap { if _ then files += fileId -> (None, Seq()) }
  }

  /** @return The data entry (created if missing) of the handle, [[None]] if handle is missing. */
  def dataEntry(fileId: Long, sizeInDb: Long => Long): Option[DataEntry] = synchronized {
    if closing then None else files.get(fileId).map {
      case (Some(current), _) => current
      case (None, storing) =>
        log.trace(s"Creating write cache for $fileId.")
        val initialSize = storing.headOption.map(_.size).getOrElse(sizeInDb(fileId))
        DataEntry(dataSeq, initialSize, tempPath)
          .tap(entry => files += fileId -> (Some(entry), storing))
    }
  }

  /** @return [[None]] if handle is missing. */
  def read(fileId: Long, offset: Long, requestedSize: Long): Option[Iterator[(Long, Either[Long, Array[Byte]])]] =
    synchronized(files.get(fileId)).map { case current -> storing =>
      // read data from current if defined
      current.map(_.read(offset, requestedSize)).getOrElse(Iterator(offset -> Left(requestedSize)))
        .flatMap {
          case (position, Left(holeSize)) =>
            // fill in holes in data by recursing through the storing sequence
            fillHoles(position, holeSize, storing)
          case data => Iterator(data)
        }
    }

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
      case Some(Some(current) -> storing) =>
        files += fileId -> (None, current +: storing)
        if storing.isEmpty then Some(current) else None
      case other =>
        ensure("WriteHandle.release", other.isDefined, s"Missing file handle (write) for file $fileId.")
        None
  }

  def removeAndGetNext(fileId: Long, dataEntry: DataEntry): Option[DataEntry] = synchronized {
    files.get(fileId) match
      case None =>
        problem("WriteHandle.removeAndGetNext.missing", s"Missing file handle (write) for file $fileId.")
        None
      case Some((current, others :+ `dataEntry`)) =>
        if others.isEmpty && current.isEmpty then files -= fileId
        else files += fileId -> (current, others)
        others.lastOption
      case Some((_, others)) =>
        problem("WriteHandle.removeAndGetNext.mismatch", s"Previous DataEntry not found for file $fileId.")
        others.lastOption
  }

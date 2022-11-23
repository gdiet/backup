package dedup
package backend

import dedup.util.ClassLogging

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicLong

class FileHandlesWrite(tempPath: Path) extends ClassLogging:
  /** file id -> (current, storing). Remember to synchronize. */
  private var files = Map[Long, (Option[DataEntry], Seq[DataEntry])]()
  private var closing = false
  private val dataSeq = AtomicLong()

  /** Stop creating new data entries.
    * @return The entries containing an open data entry. */
  def shutdown(): Map[Long, DataEntry] = synchronized {
    log.info(s"shutdown - $files") // FIXME debug
    closing = true
    files.collect { case fileId -> (Some(current), _) => fileId -> current }
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

package dedup
package backend

import dedup.server.Settings
import dedup.util.ClassLogging

import java.util.concurrent.atomic.AtomicLong

class FileHandlesWrite(settings: Settings) extends ClassLogging:
  /** file id -> (current, storing). Remember to synchronize. */
  private var files = Map[Long, (Option[DataEntry], Seq[DataEntry])]()
  private val dataSeq = AtomicLong()

  def getSize(fileId: Long): Option[Long] =
    synchronized(files.get(fileId)) match
      case None => log.error(s"Missing write handle contents for file id $fileId."); None
      case Some(Some(current) -> _) => Some(current.size)
      case Some(None -> (head +: _)) => Some(head.size)
      case Some(None -> _) => None

  def addIfMissing(fileId: Long): Boolean = synchronized {
    (!files.contains(fileId)).tap { if _ then files += fileId -> (None, Seq()) }
  }

  def dataEntry(fileId: Long, sizeInDb: Long => Long): Option[DataEntry] = synchronized {
    files.get(fileId).map {
      case (Some(current), _) => current
      case (None, storing) =>
        log.trace(s"Creating write cache for $fileId.")
        val initialSize = storing.headOption.map(_.size).getOrElse(sizeInDb(fileId))
        DataEntry(dataSeq, initialSize, settings.tempPath)
          .tap(entry => files += fileId -> (Some(entry), storing))
    }
  }

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

  /** @return The data entry to enqueue for storing. */
  def release(fileId: Long): Option[DataEntry] = synchronized {
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
        problem("WriteHandle.removeAndGetNext", s"Missing file handle (write) for file $fileId.")
        None
      case Some((current, others :+ `dataEntry`)) =>
        files += fileId -> (current, others)
        others.lastOption
      case Some((_, others)) =>
        problem("WriteHandle.removeAndGetNext", s"Previous DataEntry not found for file $fileId.")
        others.lastOption
  }

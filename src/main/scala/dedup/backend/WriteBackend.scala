package dedup
package backend

import dedup.db.WriteDatabase
import dedup.server.Settings

import java.util.concurrent.atomic.AtomicLong

/** Don't instantiate more than one backend for a repository. */
// Why not? Because the backend object is used for synchronization.
final class WriteBackend(settings: Settings, db: WriteDatabase) extends ReadBackend(settings, db):

  /** file id -> (current, storing). Remember to synchronize. */
  private var files = Map[Long, (Option[DataEntry], Seq[DataEntry])]()
  private val dataSeq = AtomicLong()

  override def shutdown(): Unit = sync {
    // TODO Write the cache before closing
    db.shutdownCompact()
    super.shutdown()
  }

  override def size(fileEntry: FileEntry): Long =
    sync(files.get(fileEntry.id)) match
      case Some(Some(current), _) => current.size
      case Some(None, head +: _) => head.size
      case _ => super.size(fileEntry)

  override def mkDir(parentId: Long, name: String): Option[Long] =
    sync { db.mkDir(parentId, name) }

  override def setTime(id: Long, newTime: Long): Unit =
    sync { db.setTime(id, newTime) }

  override def deleteChildless(entry: TreeEntry): Boolean =
    sync { if db.children(entry.id).nonEmpty then false else { db.delete(entry.id); true } }

  override def open(fileId: Long, dataId: DataId): Unit = sync {
    super.open(fileId, dataId)
    if !files.contains(fileId) then files += fileId -> (None, Seq())
  }

  override def createAndOpen(parentId: Long, name: String, time: Time): Option[Long] = sync {
    // A sensible Option.tapEach might be available in future Scala versions, see
    // https://stackoverflow.com/questions/67017901/why-does-scala-option-tapeach-return-iterable-not-option
    // and https://github.com/scala/scala-library-next/pull/80
    db.mkFile(parentId, name, time, DataId(-1)).tap(_.foreach { fileId =>
      super.open(fileId, DataId(-1))
      files += fileId -> (None, Seq())
    })
  }

  private def dataEntry(fileId: Long): Option[DataEntry] =
    sync(files.get(fileId).map {
      case (Some(current), _) => current
      case (None, storing) =>
        log.info(s"Creating write cache for $fileId.") // FIXME trace or remove
        val initialSize = storing.headOption.map(_.size).getOrElse(db.logicalSize(dataId(fileId)))
        DataEntry(dataSeq, initialSize, settings.tempPath)
          .tap(entry => files += fileId -> (Some(entry), storing))
    })

  override def write(fileId: Long, data: Iterator[(Long, Array[Byte])]): Boolean =
    dataEntry(fileId).map(_.write(data)).isDefined

  override def truncate(fileId: Long, newSize: Long): Boolean =
    dataEntry(fileId).map(_.truncate(newSize)).isDefined
  
  override def read(fileId: Long, offset: Long, requestedSize: Long): Option[Iterator[(Long, Array[Byte])]] =
    sync(files.get(fileId)).map { case current -> storing =>
      // read data from current if defined
      current.map(_.read(offset, requestedSize)).getOrElse(Iterator(offset -> Left(requestedSize)))
        .flatMap {
          case (position, Left(holeSize)) =>
            // fill in holes in data from storing (recurse through the storing sequence
            def fillHoles(position: Long, holeSize: Long, remaining: Seq[DataEntry]): Iterator[(Long, Either[Long, Array[Byte]])] =
              remaining match
                case Seq() => Iterator(offset -> Left(holeSize))
                case head +: tail =>
                  head.read(position, holeSize).flatMap {
                    case (innerPosition, Left(innerHoleSize)) => fillHoles(innerPosition, innerHoleSize, tail)
                    case other => Iterator(other)
                  }
            fillHoles(position, holeSize, storing)
          case data => Iterator(data)
        }
        .flatMap {
          case (position, Left(holeSize)) => super.read(fileId, position, holeSize).getOrElse {
            log.error(s"Reading base layer of file $fileId failed, replacing it with zeros. This is a bug.")
            Iterator.range(position, position + holeSize, memChunk.toLong).map { localPos =>
              localPos -> new Array[Byte](cache.asInt(math.min(position + holeSize - localPos, memChunk)))
            }
          }
          case (position, Right(bytes)) => Iterator(position -> bytes)
        }
    }

  override def release(fileId: Long): Boolean =
    sync(releaseInternal(fileId)) match
      case None => false
      case Some(count -> _) if count > 0 => true
      case Some(count -> dataId) => sync {
        files.get(fileId) match
          case None => log.warn("Unexpected case, should be investigated."); true // FIXME is this good?
          case Some(None -> _) => true
          case Some(Some(current) -> storing) =>
            files += fileId -> (None, current +: storing)
            log.warn(s"Write-through not yet implemented.") // FIXME remove
            true
      }

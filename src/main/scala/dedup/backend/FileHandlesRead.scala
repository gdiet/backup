package dedup
package backend

import dedup.util.ClassLogging

/* file id -> (handle count, data id). Remember to synchronize. TODO document */
class FileHandlesRead extends ClassLogging:
  private var closing = false
  private var files = Map[Long, (Int, DataId)]()

  def shutdown(): Int = synchronized { closing = true; files.size }

  def dataId(fileId: Long): Option[DataId] = synchronized(files.get(fileId)).map(_._2)

  def open(fileId: Long, dataId: DataId): Unit = synchronized {
    ensure("readhandles.open", !closing, "Attempt to open file while closing the backend.")
    files += fileId -> (files.get(fileId) match
      case None => 1 -> dataId
      case Some(count -> `dataId`) => count +1 -> dataId
      case Some(count -> other) =>
        // FIXME Check whether we can be sure enough that the data ID is the same - what happens if persisting updates the data ID?
        problem("readhandles.open.dataid.conflict", s"Open #$count - dataId $dataId differs from previous $other.")
        count +1 -> dataId
      )
  }

  def release(fileId: Long): Option[DataId] = synchronized {
    files.get(fileId) match
      case None =>
        log.warn(s"release($fileId) called for a file handle that is currently not open.")
        None
      case Some(count -> dataId) if count > 1 =>
        files += fileId -> (count - 1, dataId)
        None
      case Some(count -> dataId) =>
        if count < 1 then log.error(s"Handle count $count for id $fileId.")
        files -= fileId
        Some(dataId)
  }

  def releaseFully(fileId: Long): Option[DataId] = synchronized {
    files.get(fileId) match
      case None =>
        log.warn(s"releaseFully($fileId) called for a file handle that is currently not open.")
        None
      case Some(count -> dataId) =>
        if count < 1 then log.error(s"Handle count $count for id $fileId.")
        files -= fileId
        Some(dataId)
  }

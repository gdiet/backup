package dedup
package server

class Handles:
  private object Empty
  private object Placeholder
  private type Current = Empty.type | Placeholder.type | DataEntry

  /** file id -> (count, dataId, current, storing). Remember to synchronize. */
  private var files = Map[Long, (Int, DataId, Current, Seq[DataEntry])]()

  /** @return The size of the cached entry if any or [[None]]. */
  def cachedSize(fileId: Long): Option[Long] =
    synchronized(files.get(fileId)).flatMap {
      case (_, _, current: DataEntry, _)  => Some(current.size)
      case (_, _, Placeholder, head +: _) => Some(head.size)
      case _ => None
    }

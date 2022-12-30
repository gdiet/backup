package dedup
package server

final case class Handle(count: Int, dataId: DataId, current: Option[DataEntry] = None, persisting: Seq[DataEntry] = Seq()):
  
  /** Prevent race conditions when reading from a persisting entry while at the same time that entry is fully written,
    * gets closed thus becomes unavailable for reading. This race condition can not affect the [[current]] entry because
    * reading requires to hold a file handle preventing [[current]] to be persisted. */
  def readLock[T](f: Handle => T): T =
    persisting.foreach(_.acquire())
    try f(this) finally persisting.foreach(_.release())

  def withCurrent(entry: DataEntry): Handle = copy(current = Some(entry))

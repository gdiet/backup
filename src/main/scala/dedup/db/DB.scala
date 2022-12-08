package dedup
package db

class DB(connection: java.sql.Connection):
  // Note: Prepared statements are used synchronized because they are stateful.
  import connection.prepareStatement as prepare
  extension [T <: AnyRef](t: T) def sync[U](f: T => U): U = t.synchronized(f(t))

  private val qLogicalSize = prepare("SELECT length FROM DataEntries WHERE id = ? AND seq = 1")
  /** @return the logical file size for the data entry or 0 if there is no matching data entry. */
  def logicalSize(dataId: DataId): Long =
    qLogicalSize.sync(_.set(dataId).query(maybe(_.getLong(1))).getOrElse(0))

  // TODO check whether it would be better to return Area instead of position+size
  private val qParts = prepare("SELECT start, stop-start FROM DataEntries WHERE id = ? ORDER BY seq ASC")
  def parts(dataId: DataId): Seq[(Long, Long)] =
    qParts.sync(_.set(dataId).query(seq { rs =>
      val (start, size) = rs.getLong(1) -> rs.getLong(2)
      ensure("data.part.start", start >= 0, s"Start $start must be >= 0.")
      ensure("data.part.size", size >= 0, s"Size $size must be >= 0.")
      start -> size
    })).filterNot(_._2 == 0) // Filter parts of size 0 as created when blacklisting.

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

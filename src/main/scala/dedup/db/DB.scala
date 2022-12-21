package dedup
package db

final class DB(connection: java.sql.Connection) extends AutoCloseable:

  override def close(): Unit = connection.close()

  /** Utility class making sure a [[java.sql.PreparedStatement]] is used synchronized
    * because in many cases a [[java.sql.PreparedStatement]] is stateful.
    *
    * @param sql     The SQL string to prepare as [[java.sql.PreparedStatement]].
    * @param monitor The monitor to use for synchronization.
    *                If [[None]] (default), synchronizing on this [[prepare]] instance. */
  private class prepare(sql: String, monitor: Option[Object] = None):
    private val prep = connection.prepareStatement(sql)
    private val sync = monitor.getOrElse(this)
    def apply[T](f: java.sql.PreparedStatement => T): T = sync.synchronized(f(prep))

  /** From a [[selectTreeEntry]] query extract the [[TreeEntry]] data. */
  private def treeEntry(rs: java.sql.ResultSet): TreeEntry =
    TreeEntry(
      rs.getLong("id"),
      rs.getLong("parentId"),
      rs.getString("name"),
      Time(rs.getLong("time")),
      rs.opt(_.getLong("dataId")).map(DataId(_))
    )
  /** Select [[TreeEntry]] data that can be extracted using the [[treeEntry]] method. */
  private val selectTreeEntry = "SELECT id, parentId, name, time, dataId FROM TreeEntries"

  private val qChild = prepare(s"$selectTreeEntry WHERE parentId = ? AND name = ? AND deleted = 0")
  def child(parentId: Long, name: String): Option[TreeEntry] =
    qChild(_.set(parentId, name).query(maybe(treeEntry)))

  private val qChildren = prepare(s"$selectTreeEntry WHERE parentId = ? AND deleted = 0")
  def children(parentId: Long): Seq[TreeEntry] =
    // On linux, empty names don't work, and the root node has itself as child...
    qChildren(_.set(parentId).query(seq(treeEntry))).filterNot(_.name.isEmpty)

  private val qLogicalSize = prepare("SELECT length FROM DataEntries WHERE id = ? AND seq = 1")
  /** @return the logical file size for the data entry or 0 if there is no matching data entry. */
  def logicalSize(dataId: DataId): Long =
    qLogicalSize(_.set(dataId).query(maybe(_.getLong(1))).getOrElse(0))

  // TODO check whether it would be better to return Area instead of position+size
  private val qParts = prepare("SELECT start, stop-start FROM DataEntries WHERE id = ? ORDER BY seq ASC")
  def parts(dataId: DataId): Seq[(Long, Long)] =
    qParts(_.set(dataId).query(seq { rs =>
      val (start, size) = rs.getLong(1) -> rs.getLong(2)
      ensure("data.part.start", start >= 0, s"Start $start must be >= 0.")
      ensure("data.part.size", size >= 0, s"Size $size must be >= 0.")
      start -> size
    })).filterNot(_._2 == 0) // Filter parts of size 0 as created when blacklisting.

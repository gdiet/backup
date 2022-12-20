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

package dedup
package db

import java.sql.{Connection, ResultSet, Statement}

/** The methods of this class are not thread safe. */
// Why not? Because prepared statements are stateful.
class ReadDatabase(connection: Connection):
  import connection.{prepareStatement => prepare}

  private val qLogicalSize = prepare("SELECT length FROM DataEntries WHERE id = ? AND seq = 1")
  /** @return the logical file size for the data entry or 0 if there is no matching data entry. */
  def logicalSize(dataId: DataId): Long =
    qLogicalSize.set(dataId).query(maybe(_.getLong(1))).getOrElse(0)

  /** ResultSet: (id, parentId, name, time, dataId) */
  def treeEntry(rs: ResultSet): TreeEntry =
    TreeEntry(
      rs.getLong("id"),
      rs.getLong("parentId"),
      rs.getString("name"),
      Time(rs.getLong("time")),
      rs.opt(_.getLong("dataId")).map(DataId(_))
    )

  private val selectTreeEntry = "SELECT id, parentId, name, time, dataId FROM TreeEntries"

  private val qChild = prepare(s"$selectTreeEntry WHERE parentId = ? AND name = ? AND deleted = 0")
  def child(parentId: Long, name: String): Option[TreeEntry] = {
    qChild.set(parentId, name).query(maybe(treeEntry))
  }

  private val qChildren = prepare(s"$selectTreeEntry WHERE parentId = ? AND deleted = 0")
  def children(parentId: Long): Seq[TreeEntry] = {
    qChildren.set(parentId).query(seq(treeEntry))
  }.filterNot(_.name.isEmpty) // On linux, empty names don't work, and the root node has itself as child...



/** The methods of this class are not thread safe. */
// Why not? Because prepared statements are stateful.
final class WriteDatabase(connection: Connection) extends ReadDatabase(connection) with AutoCloseable {
  override def close(): Unit = { ??? }
}

package dedup
package db

import java.sql.{Connection, ResultSet, Statement}

/** The methods of this class are not thread safe. */
// Why not? Because prepared statements are stateful. Synchronize externally as needed.
class ReadDatabase(connection: Connection):
  import connection.prepareStatement as prepare

  private val qLogicalSize = prepare("SELECT length FROM DataEntries WHERE id = ? AND seq = 1")
  /** @return the logical file size for the data entry or 0 if there is no matching data entry. */
  def logicalSize(dataId: DataId): Long =
    qLogicalSize.sync(_.set(dataId).query(maybe(_.getLong(1))).getOrElse(0))

  /** ResultSet: (id, parentId, name, time, dataId) */
  private def treeEntry(rs: ResultSet): TreeEntry =
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

  // TODO check whether it would be better to return Area instead of position+size
  private val qParts = prepare("SELECT start, stop-start FROM DataEntries WHERE id = ? ORDER BY seq ASC")
  def parts(dataId: DataId): Seq[(Long, Long)] = {
    qParts.set(dataId).query(seq { rs =>
      val (start, size) = rs.getLong(1) -> rs.getLong(2)
      ensure("data.part.start", start >= 0, s"Start $start must be >= 0.")
      ensure("data.part.size", size >= 0, s"Size $size must be >= 0.")
      start -> size
    })
  }.filterNot(_._2 == 0) // Filter parts of size 0 as created when blacklisting.

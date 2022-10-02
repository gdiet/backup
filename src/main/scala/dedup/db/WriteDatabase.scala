package dedup
package db

import java.sql.{Connection, Statement}
import scala.util.Try
import scala.util.Using.resource

/** The methods of this class are not thread safe. */
// Why not? Because prepared statements are stateful. Synchronize externally as needed.
final class WriteDatabase(connection: Connection) extends ReadDatabase(connection) with util.ClassLogging:
  import connection.prepareStatement as prepare

  def shutdownCompact(): Unit =
    log.info("Compacting database...")
    resource(connection.createStatement())(_.execute("SHUTDOWN COMPACT"))

  private val iDir = prepare(
    "INSERT INTO TreeEntries (parentId, name, time) VALUES (?, ?, ?)", Statement.RETURN_GENERATED_KEYS
  )
  /** @return Some(id) or None if a child entry with the same name already exists. */
  def mkDir(parentId: Long, name: String): Option[Long] = Try {
    // Name conflict triggers SQL exception due to unique constraint.
    val count = iDir.set(parentId, name, now).executeUpdate()
    ensure("db.mkdir", count == 1, s"For parentId $parentId and name '$name', mkDir update count is $count instead of 1.")
    iDir.getGeneratedKeys.tap(_.next()).getLong("id")
  }.toOption

  private val uTime = prepare(
    "UPDATE TreeEntries SET time = ? WHERE id = ?"
  )
  /** Sets the last modified time stamp for a tree entry. Should be called only for existing entry IDs. */
  def setTime(id: Long, newTime: Long): Unit =
    val count = uTime.set(newTime, id).executeUpdate()
    ensure("db.set.time", count == 1, s"For id $id, setTime update count is $count instead of 1.")

  private val dTreeEntry = prepare(
    "UPDATE TreeEntries SET deleted = ? WHERE id = ?"
  )
  /** Deletes a tree entry. Should be called only for existing entry IDs. */
  def delete(id: Long): Unit =
    val count = dTreeEntry.set(now.nonZero, id).executeUpdate()
    ensure("db.delete", count == 1, s"For id $id, delete count is $count instead of 1.")

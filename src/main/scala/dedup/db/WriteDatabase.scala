package dedup
package db

import java.sql.{Connection, Statement}
import scala.util.Try

/** The methods of this class are not thread safe. */
// Why not? Because prepared statements are stateful. Synchronize externally as needed.
final class WriteDatabase(connection: Connection) extends ReadDatabase(connection) with util.ClassLogging:
  import connection.prepareStatement as prepare
  private val statement: Statement = connection.createStatement()

  def shutdownCompact(): Unit =
    log.info("Compacting database...")
    statement.execute("SHUTDOWN COMPACT;")

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

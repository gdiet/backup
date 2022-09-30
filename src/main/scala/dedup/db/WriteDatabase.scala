package dedup
package db

import java.sql.Connection

/** The methods of this class are not thread safe. */
// Why not? Because prepared statements are stateful. Synchronize externally as needed.
final class WriteDatabase(connection: Connection) extends ReadDatabase(connection) with AutoCloseable {
  override def close(): Unit = { ??? }
}

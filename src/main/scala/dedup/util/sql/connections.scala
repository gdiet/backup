package dedup.util.sql

import java.sql.Connection

trait ConnectionProvider extends AutoCloseable {
  def connection[T](f: Connection => T): T
}

class SingleConnection(val c: Connection) extends ConnectionProvider {
  override def connection[T](f: Connection => T): T = synchronized(f(c))
  override def close(): Unit = c.close()
}

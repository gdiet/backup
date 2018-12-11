package dedup.util.sql

import java.sql.Connection

trait ConnectionProvider extends AutoCloseable {
  def use[T](f: Connection => T): T
}

class SingleConnection(factory: () => Connection) extends ConnectionProvider {
  private val connection = factory()
  override def use[T](f: Connection => T): T = synchronized(f(connection))
  override def close(): Unit = connection.close()
}

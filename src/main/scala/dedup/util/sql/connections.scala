package dedup.util.sql

import java.sql.{Connection, Statement}

import scala.util.Using

trait ConnectionProvider extends AutoCloseable {
  def con[T](f: Connection => T): T
  def stat[T](f: Statement => T): T = con(c => Using(c.createStatement())(f).get)
}

class SingleConnection(factory: () => Connection) extends ConnectionProvider {
  private val connection = factory()
  override def con[T](f: Connection => T): T = synchronized(f(connection))
  override def close(): Unit = connection.close()
}

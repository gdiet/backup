package net.diet_rich.util.sql

import java.sql.{Connection, DriverManager}

import net.diet_rich.util.ArmThreadLocal
import net.diet_rich.util.init
import net.diet_rich.util.valueOf

class ConnectionFactory(factory: () => Connection, onClose: Connection => Unit)
  extends ArmThreadLocal[Connection] (
    factory,
    {
      case head +: tail =>
        try { tail foreach (_.close()) }
        finally { onClose(head); head.close() }
      case Vector() => ()
    }
  ) {
  def transaction[T](f: => T): T =
    try { valueOf(f).before(this().commit()) } catch { case t: Throwable => this().rollback(); throw t }
}

object ConnectionFactory {
  def apply(url: String, user: String, password: String, executeOnClose: Option[String], autoCommit: Boolean): ConnectionFactory = {
    new ConnectionFactory(
      () => init(DriverManager.getConnection(url, user, password))(_.setAutoCommit(autoCommit)),
      connection => executeOnClose foreach { connection.prepareStatement(_).execute() }
    )
  }
}

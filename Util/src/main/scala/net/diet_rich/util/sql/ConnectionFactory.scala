package net.diet_rich.util.sql

import java.sql.{Connection, DriverManager}

import net.diet_rich.util.ArmThreadLocal
import net.diet_rich.util.init

class ConnectionFactory(factory: () => Connection, onClose: Connection => Unit)
  extends ArmThreadLocal[Connection] (
    factory,
    {
      case head +: tail =>
        try { tail foreach (_.close()) }
        finally { onClose(head); head.close() }
      case Vector() => ()
    }
  )

object ConnectionFactory {
  def apply(url: String, user: String, password: String, executeOnClose: Option[String], autoCommit: Boolean): ConnectionFactory = {
    new ConnectionFactory(
      () => init(DriverManager.getConnection(url, user, password))(_.setAutoCommit(autoCommit)),
      connection => executeOnClose foreach { connection.prepareStatement(_).execute() }
    )
  }
}

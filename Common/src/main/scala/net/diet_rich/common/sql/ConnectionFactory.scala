package net.diet_rich.common.sql

import java.sql.{DriverManager, Connection}

import net.diet_rich.common._

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
  def apply(driver: String, url: String, user: String, password: String, executeOnClose: Option[String]): ConnectionFactory = {
    Class forName driver
    new ConnectionFactory(
      () => DriverManager.getConnection(url, user, password),
      connection => executeOnClose foreach { connection.prepareStatement(_).execute() }
    )
  }
}

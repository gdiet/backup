package net.diet_rich.common.sql

import java.sql.{DriverManager, Connection}

import scala.collection.JavaConverters._

import net.diet_rich.common._

class ConnectionFactory(factory: () => Connection, onClose: Connection => Unit = _ => ())
  extends ArmThreadLocal[Connection] (
    factory,
    _.asScala.toList match {
      case head :: tail =>
        tail foreach (_.close())
        onClose(head)
        head.close()
      case Nil => ()
    }
  )

object ConnectionFactory {
  def apply(driver: String, url: String, user: String, password: String, executeOnShutdown: Option[String]): ConnectionFactory = {
    Class forName driver
    new ConnectionFactory(
      () => DriverManager.getConnection(url, user, password),
      connection => executeOnShutdown foreach { connection.prepareStatement(_).execute() }
    )
  }
}

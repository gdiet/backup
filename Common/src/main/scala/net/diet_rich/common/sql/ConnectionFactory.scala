package net.diet_rich.common.sql

import java.sql.{DriverManager, Connection}
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConverters._

import net.diet_rich.common._

class ConnectionFactory(factory: => Connection, onClose: Connection => Unit = _ => ()) extends ScalaThreadLocal[Connection] with AutoCloseable {
  private var isOpen = true
  private val allConnections = new ConcurrentLinkedQueue[Connection]()
  private val threadLocal = ScalaThreadLocal {
    synchronized {
      require(isOpen, "ConnectionFactory is already closed.")
      init(factory){allConnections add _}
    }
  }
  override def apply(): Connection = threadLocal.apply()
  override def close(): Unit = synchronized {
    isOpen = false
    allConnections.asScala.toList match {
      case head :: tail =>
        tail foreach (_.close())
        onClose(head)
        head.close()
      case Nil => ()
    }
  }
}

object ConnectionFactory {
  def apply(driver: String, url: String, user: String, password: String, executeOnShutdown: Option[String]): ConnectionFactory = {
    Class forName driver
    new ConnectionFactory(
      DriverManager.getConnection(url, user, password),
      connection => executeOnShutdown foreach { connection.prepareStatement(_).execute() }
    )
  }
}

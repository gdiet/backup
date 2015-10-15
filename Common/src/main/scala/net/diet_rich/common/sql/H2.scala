package net.diet_rich.common.sql

import java.sql.DriverManager

object H2 {
  val driver = "org.h2.Driver"
  val user = "sa"
  val password = ""
  val onShutdown = "SHUTDOWN COMPACT"

  Class forName driver

  def memoryFactory(dbName: String = "test"): ConnectionFactory =
    new ConnectionFactory(DriverManager getConnection (s"jdbc:h2:mem:$dbName", user, password))
}

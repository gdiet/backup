package net.diet_rich.common.sql

import java.io.File
import java.sql.DriverManager

object H2 {
  Class.forName("org.h2.Driver")

  def memoryFactory(dbName: String = "test"): ConnectionFactory =
    new ConnectionFactory(DriverManager.getConnection(s"jdbc:h2:mem:$dbName", "sa", ""))

  def fileFactory(parentDir: File, dbName: String): ConnectionFactory =
    new ConnectionFactory(
      DriverManager.getConnection(s"jdbc:h2:${parentDir.getAbsolutePath}/$dbName;DB_CLOSE_ON_EXIT=FALSE", "sa", ""),
      { _.createStatement() execute "SHUTDOWN COMPACT" }
    )
}

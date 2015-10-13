package net.diet_rich.common.sql

import java.io.File
import java.sql.DriverManager

object H2 {
  val driver = "org.h2.Driver"
  val user = "sa"
  val password = ""
  val onShutdown = "SHUTDOWN COMPACT"

  Class forName driver

  def memoryFactory(dbName: String = "test"): ConnectionFactory =
    new ConnectionFactory(DriverManager.getConnection(s"jdbc:h2:mem:$dbName", user, password))

  // FIXME not needed anymore, see connectionFactory in package object
  def fileFactory(parentDir: File, dbName: String): ConnectionFactory =
    new ConnectionFactory(
    // TODO Eventually check whether the old format ;MV_STORE=FALSE;MVCC=FALSE is still way more compact or not.
    // For details see http://code.google.com/p/h2database/issues/detail?id=542
    DriverManager.getConnection(s"jdbc:h2:${parentDir.getAbsolutePath}/$dbName;DB_CLOSE_ON_EXIT=FALSE", user, password),
      { _.createStatement() execute onShutdown }
    )
}

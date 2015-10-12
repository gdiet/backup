package net.diet_rich.common.sql

import java.io.File
import java.sql.DriverManager

object H2 {
  Class.forName("org.h2.Driver")

  def memoryFactory(dbName: String = "test"): ConnectionFactory =
    new ConnectionFactory(DriverManager.getConnection(s"jdbc:h2:mem:$dbName", "sa", ""))

  def fileFactory(parentDir: File, dbName: String): ConnectionFactory =
    new ConnectionFactory(
    // TODO Eventually check whether the old format ;MV_STORE=FALSE;MVCC=FALSE is still way more compact or not.
    // For details see http://code.google.com/p/h2database/issues/detail?id=542
    DriverManager.getConnection(s"jdbc:h2:${parentDir.getAbsolutePath}/$dbName;DB_CLOSE_ON_EXIT=FALSE", "sa", ""),
      { _.createStatement() execute "SHUTDOWN COMPACT" }
    )
}

package dedup

import java.sql.{Connection, DriverManager}

object H2 {
  Class forName "org.h2.Driver"

  // For SQL debugging, add to the DB URL "...;TRACE_LEVEL_SYSTEM_OUT=2"
  private def jdbcUrl(directory: java.io.File, readonly: Boolean) =
    if (readonly) s"jdbc:h2:$directory/dedupfs;ACCESS_MODE_DATA=r" else s"jdbc:h2:$directory/dedupfs"

  def file(directory: java.io.File, readonly: Boolean, settings: String = ""): Connection = {
    if (!readonly) new java.io.File(directory, "fsdb/dedupfs.trace.db")
      .tap(file => require(!file.exists(), s"Database trace file $file found. Check for database problems."))
    DriverManager.getConnection(jdbcUrl(directory, readonly) + settings, "sa", "").tap(_.setAutoCommit(true))
  }

  def mem(): Connection =
    DriverManager.getConnection("jdbc:h2:mem:", "sa", "").tap(_.setAutoCommit(true))
}

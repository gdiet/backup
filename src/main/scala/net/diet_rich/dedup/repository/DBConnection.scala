// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.repository

import java.sql._

object DBConnection {
  def forH2(dbpath: String, readonly: Boolean, enableShutdownHook: Boolean): Connection = {
    // for enableShutdownHook = true, connection is closed by H2's built-in shutdown hook when VM exits normally.
    Class forName "org.h2.Driver"
    val url = s"jdbc:h2:$dbpath" + (if (readonly) ";ACCESS_MODE_DATA=r" else "") + (if (!enableShutdownHook) ";DB_CLOSE_ON_EXIT=FALSE" else "")
    val connection = DriverManager getConnection(url, "sa", "")
    connection setAutoCommit true
    connection
  }
}

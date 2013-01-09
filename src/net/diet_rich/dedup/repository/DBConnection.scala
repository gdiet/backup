// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.repository

import java.sql._

object DBConnection {
  def forH2(dbpath: String): Connection = {
    // Connection is closed by H2's built-in shutdown hook when VM exits normally.
    Class forName "org.h2.Driver"
    val connection = DriverManager getConnection(s"jdbc:h2:$dbpath", "sa", "")
    connection setAutoCommit true
    connection
  }
}
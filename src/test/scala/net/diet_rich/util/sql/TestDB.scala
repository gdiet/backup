// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.sql

import java.sql.{Connection, DriverManager}
import net.diet_rich.util.init
import scala.util.Random

class ImplicitConnection(implicit val connection: Connection)

object TestDB {
  def h2mem: Connection = {
    Class forName "org.h2.Driver"
    init(DriverManager getConnection("jdbc:h2:mem:test" + Random.nextString(10), "sa", "")) {
      _ setAutoCommit true
    }
  }
}

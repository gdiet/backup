// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.database

import java.sql.Connection
import java.sql.DriverManager

import scala.util.Random

import net.diet_rich.util.init

object TestDB {
  def h2mem: Connection = {
    Class forName "org.h2.Driver"
    init(DriverManager getConnection("jdbc:h2:mem:test" + Random.nextString(10), "sa", "")) {
      _ setAutoCommit true
    }
  }
}

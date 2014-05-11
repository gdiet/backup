// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import scala.slick.driver.H2Driver.simple._

object InMemoryDatabase {
  private val dbId = new java.util.concurrent.atomic.AtomicLong()

  def withEmptyDB[T] (f: Database => T): T = {
    val database = Database forURL (
      // ;TRACE_LEVEL_SYSTEM_OUT=2 or 3 for console debug output
      url = s"jdbc:h2:mem:testdb_${dbId.incrementAndGet()}}",
      user = "sa", password = "", driver = "org.h2.Driver"
    )
    val connection = database createConnection
    val result = try {
      f(database)
    } finally {
      connection close()
    }
    result
  }

  def withDB[T] (f: Database => T): T = withEmptyDB { database =>
    database.withSession { implicit session =>
      SQLTables createTables 16
      SQLTables recreateIndexes
    }
    f(database)
  }
}

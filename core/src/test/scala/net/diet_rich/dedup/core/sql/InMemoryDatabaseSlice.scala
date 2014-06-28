// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.sql

import scala.slick.driver.H2Driver.simple._

trait InMemoryDatabaseSlice extends DatabasePart {
  override protected val database = Database forURL (
    // ;TRACE_LEVEL_SYSTEM_OUT=2 or 3 for console debug output
    url = s"jdbc:h2:mem:testdb_${InMemoryDatabaseSlice.dbId incrementAndGet}",
    user = "sa", password = "", driver = "org.h2.Driver"
  )
}

object InMemoryDatabaseSlice {
  private val dbId = new java.util.concurrent.atomic.AtomicLong()
}

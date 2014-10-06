// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.sql

import scala.slick.driver.H2Driver.simple.Database

trait InMemoryDbPart extends ThreadSpecificSessionsPart {
  override val database: CurrentDatabase = Database forURL(
    // ;TRACE_LEVEL_SYSTEM_OUT=2 or 3 for console debug output
    url = s"jdbc:h2:mem:testdb_${InMemoryDbPart.dbId incrementAndGet}",
    user = "sa", password = "", driver = "org.h2.Driver"
  )
}

object InMemoryDbPart {
  private val dbId = new java.util.concurrent.atomic.AtomicLong()
}

trait InMemoryDBPartWithTables extends InMemoryDbPart {
  DBUtilities createTables "MD5"
  DBUtilities recreateIndexes
}

// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.sql

import scala.slick.driver.H2Driver.simple._

object InMemoryDB {
  private val dbId = new java.util.concurrent.atomic.AtomicLong()

  def providing[T](f: SessionProvider => T): T = f(new SessionProvider {
    implicit val session: Session = Database forURL(
      // ;TRACE_LEVEL_SYSTEM_OUT=2 or 3 for console debug output
      url = s"jdbc:h2:mem:testdb_${dbId incrementAndGet}",
      user = "sa", password = "", driver = "org.h2.Driver"
    ) createSession()
    DBUtilities createTables 16
    DBUtilities recreateIndexes
  })
}

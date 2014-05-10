// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import scala.slick.driver.H2Driver.simple._

import net.diet_rich.dedup.util.ThreadSpecific

class MemoryDbWithTables {
  import InMemoryDatabase._
  protected lazy val database = Database forURL (
    url = s"jdbc:h2:mem:testdb_${dbId.incrementAndGet()}};TRACE_LEVEL_SYSTEM_OUT=2",
    user = "sa", password = "", driver = "org.h2.Driver"
  )
  val connection = database.createConnection() // FIXME only needed to prevent the database from closing prematurely
  database.withSession { implicit session =>
    SQLTables createTables 16
    SQLTables recreateIndexes
  }
  protected lazy val sessions: ThreadSpecific[Session] = ThreadSpecific (database createSession)
}

trait InMemoryDatabase {
  import InMemoryDatabase._
  protected lazy val database = Database forURL (
    url = s"jdbc:h2:mem:testdb_${dbId.incrementAndGet()}};TRACE_LEVEL_SYSTEM_OUT=2",
    user = "sa", password = "", driver = "org.h2.Driver"
  )
  protected lazy val sessions: ThreadSpecific[Session] = ThreadSpecific (database createSession)
}

object InMemoryDatabase {
  val dbId = new java.util.concurrent.atomic.AtomicLong()
}

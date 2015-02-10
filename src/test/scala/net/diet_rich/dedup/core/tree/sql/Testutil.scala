package net.diet_rich.dedup.core.tree.sql

import java.util.concurrent.atomic.AtomicLong

import scala.slick.driver.H2Driver.simple.Database

object Testutil {
  private val dbid = new AtomicLong(0L)

  def memoryDB: SQLSession = SQLSession(
    Database forURL(
      // ;TRACE_LEVEL_SYSTEM_OUT=2 or 3 for console debug output
      url = s"jdbc:h2:mem:testdb_${dbid incrementAndGet}",
      user = "sa", driver = "org.h2.Driver"
    )
  )
}

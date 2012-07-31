package net.diet_rich.test.util.sql

import net.diet_rich.util.sql._
import org.fest.assertions.Assertions.assertThat
import org.testng.annotations.Test

class AnExampleUsageTest {

  @Test
  def example: Unit = {
    implicit val con = DBConnection h2MemoryDB (getClass getCanonicalName)
    execUpdate(con, "CREATE TABLE table (key INT, value BIGINT);")
    val insert = prepareUpdate("INSERT INTO table VALUES (?, ?);")
    val query = prepareQuery("SELECT value FROM table ORDER BY key ASC;")
    insert(1, 7)
    insert(2, None)
    insert(3, Some(-4))
    val entries = query()(_ longOption 1).toList
    assertThat(entries) isEqualTo List(Some(7), None, Some(-4))
  }
}
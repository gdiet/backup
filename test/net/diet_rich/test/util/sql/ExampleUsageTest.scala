package net.diet_rich.test.util.sql

import org.fest.assertions.Assertions.assertThat
import org.testng.annotations.Test
import net.diet_rich.util.sql._

class ExampleUsageTest {

//  private def storeAndGetForType[T](table: String, colType: String, processor: WrappedSQLResult => T, input: List[T]): List[T] = {
//    execUpdate(con, "CREATE TABLE %s (key INT, value %s);" format (table, colType))
//    input.zipWithIndex.foreach { case (value, index) =>
//      execUpdate(con, "INSERT INTO %s VALUES (?, ?);" format table, index, value)
//    }
//    execQuery(con, "SELECT value FROM %s ORDER BY key ASC;" format table)(processor).toList
//  }
  
  @Test
  def example: Unit = {
    val con = DBConnection h2MemoryDB (getClass getCanonicalName)
    execUpdate(con, "CREATE TABLE table (key INT, value BIGINT);")
    val insert = con prepareStatement "INSERT INTO table VALUES (?, ?);"
    execUpdate(insert, 1, 7)
    execUpdate(insert, 2, None)
    execUpdate(insert, 3, Some(-4))
    val entries = execQuery(con, "SELECT value FROM table ORDER BY key ASC;")(_ longOption 1).toList
    assertThat(entries) isEqualTo List(Some(7), None, Some(-4))
  }
}
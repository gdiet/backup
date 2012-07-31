package net.diet_rich.test.util.sql

import org.fest.assertions.Assertions.assertThat
import org.testng.annotations.Test
import net.diet_rich.util.sql._
import net.diet_rich.test.TestUtil.expectThat
import net.diet_rich.test.TestHelpers

class ResultDetailsTest extends TestHelpers {

  implicit lazy val con = {
    val con = DBConnection.h2MemoryDB(className)
    execUpdate(con, "CREATE TABLE NextOption (key INT, value BIGINT);")
    execUpdate(con, "INSERT INTO NextOption VALUES (1, 7);")
    execUpdate(con, "INSERT INTO NextOption VALUES (2, 4);")
    con
  }
  
  lazy val query = prepareQuery("SELECT value FROM NextOption ORDER BY key ASC;")

  @Test
  def testNextOption: Unit = {
    val result = query()(_ long 1)
    assertThat(result.nextOption) isEqualTo Some(7)
    assertThat(result.nextOption) isEqualTo Some(4)
    assertThat(result.nextOption) isEqualTo None
    assertThat(result.nextOption) isEqualTo None
  }

  @Test
  def testNextOptionOnly: Unit = {
    val result = query()(_ long 1)
    expectThat(result.nextOptionOnly) doesThrow new IllegalStateException
    assertThat(result.nextOptionOnly) isEqualTo Some(4)
    assertThat(result.nextOptionOnly) isEqualTo None
    assertThat(result.nextOptionOnly) isEqualTo None
  }

  @Test
  def testNextOnly: Unit = {
    val result = query()(_ long 1)
    expectThat(result.nextOnly) doesThrow new IllegalStateException
    assertThat(result.nextOnly) isEqualTo 4
    expectThat(result.nextOnly) doesThrow new NoSuchElementException
  }
  
}
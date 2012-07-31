package net.diet_rich.test.util.sql

import java.sql.Connection
import net.diet_rich.util.sql._
import net.diet_rich.test.TestUtil.expectThat
import net.diet_rich.test.TestHelpers
import org.fest.assertions.Assertions.assertThat
import org.testng.annotations.Test
import java.io.File

class DatabasesTest extends TestHelpers {

  def basicDBTest(con: Connection) = {
    execUpdate(con, "CREATE TABLE NextOption (key INT, value BIGINT);")
    execUpdate(con, "INSERT INTO NextOption VALUES (1, 7);")
    execUpdate(con, "INSERT INTO NextOption VALUES (2, 4);")
    val result = execQuery(con, "SELECT value FROM NextOption ORDER BY key ASC;")(_ long 1).toList
    assertThat(result) isEqualTo List(7,4)
  }

  def dBdir(name: String) = testDir(name) + "/db"
  
  @Test
  def testH2mem: Unit =
    basicDBTest(DBConnection h2MemoryDB (className))

  @Test
  def testH2file: Unit =
    basicDBTest(DBConnection h2FileDB(dBdir("testH2file")))
    
  @Test
  def testHsqlMem: Unit =
    basicDBTest(DBConnection hsqlMemoryDB (className))

  @Test
  def testHsqlFile: Unit =
    basicDBTest(DBConnection hsqlFileDB(dBdir("testHsqlFile")))

}
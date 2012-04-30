// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

import org.fest.assertions.Assertions.assertThat
import org.testng.annotations.DataProvider
import org.testng.annotations.Test
import net.diet_rich.sb.SqlDBCommon._
import net.diet_rich.sb.DBConnection
import java.sql.Connection
import sql._

class SQLTests {
  
  protected def prepareUpdate(connection: Connection, statement: String) : Update =
    new Update {
      protected val prepared = connection prepareStatement statement
      override def apply(args: Any*): Int =
        execUpdate(prepared, args:_*)
    }

  protected def prepareQuery(connection: Connection, statement: String) : Query =
    new Query {
      protected val prepared = connection prepareStatement statement
      override def apply[T](args: Any*)(processor: WrappedSQLResult => T): ResultIterator[T] =
        execQuery(prepared, args:_*)(processor)
    }
  
  val db1 = DBConnection.hsqlMemoryDB
//  val db1 = DBConnection.hsqlFileDB(new java.io.File("temp/temp"))
  val db2 = DBConnection.h2MemoryDB

  @DataProvider(name = "dbs")
  def dbs: Array[Array[Object]] = Array(Array(db1), Array(db2))

  @Test(dataProvider = "dbs")
  def noneAsNullTest(db: Connection) = {
    execUpdate(db, "CREATE TABLE noneAsNullTest(t_string VARCHAR(256), t_long BIGINT, t_int INT);")
    val insert = prepareUpdate(db, "INSERT INTO noneAsNullTest VALUES (?, ?, ?);")
    assertThat(insert(Some("string"), Some(123L), Some(456))) isEqualTo 1
    assertThat(insert("a", None, None)) isEqualTo 1
    assertThat(insert(None, 444, None)) isEqualTo 1
    val query = prepareQuery(db, "SELECT * FROM noneAsNullTest WHERE t_string = ?;")
    val result1 = query("string"){result => (result stringOption 1, result longOption 2, result intOption 3)}.headOnly
    assertThat(result1 == (Some("string"), Some(123L), Some(456))).isTrue
    val result2 = query(Some("string")){result => (result stringOption 1, result long 2, result intOption 3)}.headOnly
    assertThat(result2 == (Some("string"), 123L, Some(456))).isTrue
    val result3 = query("a"){result => (result stringOption 1, result longOption 2, result intOption 3)}.headOnly
    assertThat(result3 == (Some("a"), None, None)).isTrue
    val query4 = prepareQuery(db, "SELECT * FROM noneAsNullTest WHERE t_string is NULL;")
    val result4 = query4(){result => (result stringOption 1, result longOption 2, result intOption 3)}.headOnly
    assertThat(result4 == (None, Some(444L), None)).isTrue
  }
  
}
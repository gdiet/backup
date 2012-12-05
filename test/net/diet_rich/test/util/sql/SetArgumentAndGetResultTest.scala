package net.diet_rich.test.util.sql

import net.diet_rich.util.sql._
import net.diet_rich.test.TestUtil.expectThat
import net.diet_rich.test.TestHelpers
import org.fest.assertions.Assertions.assertThat
import org.testng.annotations.Test

class SetArgumentAndGetResultTest extends TestHelpers {

  lazy val con = DBConnection.h2MemoryDB(className)

  private def testStoreAndGetForSimpleType[T](table: String, colType: String, processor: WrappedSQLResult => T, input: List[T]): Unit =
    assertThat(storeAndGetForType(table, colType, processor, input)).isEqualTo(input)

  private def storeAndGetForType[T](table: String, colType: String, processor: WrappedSQLResult => T, input: List[T]): List[T] = {
    execUpdate(con, "CREATE TABLE %s (key INT, value %s);" format (table, colType))
    input.zipWithIndex.foreach { case (value, index) =>
      execUpdate(con, "INSERT INTO %s VALUES (?, ?);" format table, index, value)
    }
    execQuery(con, "SELECT value FROM %s ORDER BY key ASC;" format table)(processor).toList
  }
  
  @Test
  def testSetNoArguments: Unit = testStoreAndGetForSimpleType(
    "EmptyTable", "BIGINT", {_ long 1},
    List()
  )

  @Test
  def testUnsupportedType: Unit = expectThat(
    testStoreAndGetForSimpleType(
      "UnsupportedTypeTable", "BIGINT", {_ long 1},
      List(true)
    )
  ).doesThrow(new IllegalArgumentException("setArguments does not support .* type arguments"))
  
  @Test
  def testForLong: Unit = testStoreAndGetForSimpleType(
    "LongTable", "BIGINT", {_ long 1},
    List(1,-1,Long.MaxValue,Long.MinValue)
  )

  @Test
  def testForLongOption: Unit = testStoreAndGetForSimpleType(
    "LongOptionTable", "BIGINT", {_ longOption 1},
    List(Some(1), None, Some(Long.MinValue))
  )
    
  @Test
  def testForInt: Unit = testStoreAndGetForSimpleType(
    "IntTable", "INT", {_ int 1},
    List(1,-1,Int.MaxValue,Int.MinValue)
  )

  @Test
  def testForIntOption: Unit = testStoreAndGetForSimpleType(
    "IntOptionTable", "INT", {_ intOption 1},
    List(Some(1), None)
  )
    
  @Test
  def testForString: Unit = testStoreAndGetForSimpleType(
   "StringTable", "VARCHAR(255)", {_ string 1},
    List("","abc")
  )

  @Test
  def testForStringOption: Unit = testStoreAndGetForSimpleType(
    "StringOptionTable", "VARCHAR(255)", {_ stringOption 1},
    List(None, Some(""), Some("abc"))
  )
  
  @Test
  def testForBytes: Unit = {
    val input = List(Array[Byte](),Array[Byte](5,3,2,1)) 
    val result = storeAndGetForType("BytesTable", "VARBINARY(255)", {_ bytes 1}, input)
    assertThat(input.size) isEqualTo result.size
    input.zip(result).foreach {e => assertThat(e._1.sameElements(e._2))}
  }

  @Test
  def testForBytesOption: Unit = {
    val input = List(Some(Array[Byte]()), None, Some(Array[Byte](5,3,2,1))) 
    val output = List(Some(Array[Byte]()), None, Some(Array[Byte](5,3,2,3))) 
    val result = storeAndGetForType("BytesOptionTable", "VARBINARY(255)", {_ bytesOption 1}, input)
    assertThat(input.size) isEqualTo result.size
    output.zip(result).foreach {e =>
      if (e._1.isEmpty) assertThat(e._2.isEmpty).isTrue else assertThat(e._1.get.sameElements(e._2.get))
    }
  }
  
}
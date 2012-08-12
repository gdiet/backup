package net.diet_rich.test.util.io

import net.diet_rich.util.io._
import org.fest.assertions.Assertions.assertThat
import org.testng.annotations.Test
import org.testng.annotations.Test
import java.io.ByteArrayInputStream

class PkgTest {

  @Test
  def readFullyNoInputMinusOne {
    val input = new Object { def read(bytes: Array[Byte], offset: Int, length: Int): Int = -1 }
    val read = fillFrom(input, new Array[Byte](10), 0, 10)
    assertThat(read) isEqualTo 0
    val skipped = readAll(input)
    assertThat(skipped) isEqualTo 0
  }

  @Test
  def readFullyNoInputZero {
    val input = new Object { def read(bytes: Array[Byte], offset: Int, length: Int): Int = 0 }
    val read = fillFrom(input, new Array[Byte](10), 0, 10)
    assertThat(read) isEqualTo 0
    val skipped = readAll(input)
    assertThat(skipped) isEqualTo 0
  }

  @Test
  def readFullyTwoBlocks {
    val input = new Object {
      var call = 3
      def read(bytes: Array[Byte], offset: Int, length: Int): Int = {
        call = call - 1
        call
      }
    }
    val read = fillFrom(input, new Array[Byte](10), 0, 10)
    assertThat(read) isEqualTo 3
    input.call = 3
    val skipped = readAll(input)
    assertThat(skipped) isEqualTo 3
  }
  
  @Test
  def readFullyShortOneBlockInput {
    val input = new ByteArrayInputStream(new Array[Byte](5))
    val read = fillFrom(input, new Array[Byte](10), 0, 10)
    assertThat(read) isEqualTo 5
  }

  @Test
  def readFullyLongOneBlockInput {
    val input = new ByteArrayInputStream(new Array[Byte](15))
    val read = fillFrom(input, new Array[Byte](10), 0, 10)
    assertThat(read) isEqualTo 10
  }

}
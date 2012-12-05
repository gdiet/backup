package net.diet_rich.test.util.io

import net.diet_rich.util.io._
import net.diet_rich.test.TestUtil.expectThat
import org.fest.assertions.Assertions.assertThat
import org.testng.annotations.Test
import org.testng.annotations.Test
import java.io.IOException

class UsingTests {

  @Test
  def usingClosesNormally {
    var called = false
    val input = new Object { def close(): Unit = called = true }
    using(input) { input => }
    assertThat(called).isTrue
  }

  @Test
  def usingClosesInCaseOfException {
    var called = false
    val input = new Object { def close(): Unit = called = true }
    expectThat {
      using(input) { input => throw new IOException }
    } doesThrow(new IOException)
    assertThat(called).isTrue
  }
  
}
package net.diet_rich.test.util

import org.fest.assertions.Assertions.assertThat
import org.testng.annotations.Test
import net.diet_rich.util.ScalaThreadLocal

class ScalaThreadLocalTest {

  @Test
  def defaultNameOfThreadLocal: Unit =
    assertThat(ScalaThreadLocal(new Array[Int](1)) toString) isEqualTo ""
  
}
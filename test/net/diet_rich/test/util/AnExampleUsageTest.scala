package net.diet_rich.test.util

import org.fest.assertions.Assertions.assertThat
import org.testng.annotations.Test
import net.diet_rich.util.ScalaThreadLocal

class AnExampleUsageTest {

  @Test
  def scalaThreadLocalExample: Unit = {
    val localArray = ScalaThreadLocal(new Array[Int](1), "optionalName")    
    val result = concurrent.ops.par (
        {
          assertThat(localArray(0)) isEqualTo 0
          localArray(0) = 10
          assertThat(localArray(0)) isEqualTo 10
        },
        {
          assertThat(localArray(0)) isEqualTo 0
          localArray(0) = 10
          assertThat(localArray(0)) isEqualTo 10
        }
    )
    assertThat(localArray.toString) isEqualTo "optionalName"
  }
  
}
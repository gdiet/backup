package net.diet_rich.test.util

import org.fest.assertions.Assertions.assertThat
import org.testng.annotations.Test
import net.diet_rich.util._

class AnExampleUsageTest {

  @Test
  def scalaThreadLocalExample: Unit = {
    val localArray = ScalaThreadLocal(new Array[Int](1), "optionalName")
    import concurrent.ExecutionContext.Implicits.global
    // FIXME does this test anything?
    concurrent.future {
      assertThat(localArray(0)) isEqualTo 0
      localArray(0) = 20
      Thread.sleep(100)
      assertThat(localArray(0)) isEqualTo 20
    }
    concurrent.future {
      Thread.sleep(50)
      assertThat(localArray(0)) isEqualTo 0
      localArray(0) = 10
      Thread.sleep(50)
      assertThat(localArray(0)) isEqualTo 10
    }
    assertThat(localArray.toString) isEqualTo "optionalName"
  }

  @Test
  def executorExample: Unit = {
    val executor = Executor(4,4)
    executor{ /* some code to execute in a thread pool. */ }
    executor{ /* some code to execute in a thread pool. */ }
    executor.shutdownAndAwaitTermination
  }
  
}
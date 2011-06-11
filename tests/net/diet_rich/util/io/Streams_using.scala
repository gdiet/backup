package net.diet_rich.util.io

import org.testng.annotations.Test
import org.mockito.Mockito._

class Streams_using {
  
  @Test
  def testUsingClosesNormally : Unit = {
    val closeable = mock(classOf[java.io.Closeable])
    Streams.using(closeable) {
      // do nothing
    }
    verify(closeable, times(1)) close
  }

   private class TestError extends Error
 
  @Test(expectedExceptions = Array(classOf[TestError]))
  def testUsingClosesOnError : Unit = {
    val closeable = mock(classOf[java.io.Closeable])
    Streams.using(closeable) {
      throw new TestError
    }
    verify(closeable, times(1)) close
  }

}
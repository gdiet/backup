// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.io

import org.testng.annotations.Test
import org.mockito.Mockito._

class Streams_using {
  
  @Test
  def test_using_closes_normally : Unit = {
    val closeable = mock(classOf[java.io.Closeable])
    Streams.using(closeable) {
      // here, we'd work with the closeable object
      // do nothing
    }
    verify(closeable, times(1)) close
  }

   private class TestError extends Error
 
  @Test(expectedExceptions = Array(classOf[TestError]))
  def test_using_closes_on_error : Unit = {
    val closeable = mock(classOf[java.io.Closeable])
    Streams.using(closeable) {
      // here, we'd work with the closeable object
      throw new TestError
    }
    verify(closeable, times(1)) close
  }

}
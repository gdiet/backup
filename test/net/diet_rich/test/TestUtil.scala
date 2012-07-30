// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.test

import org.testng.annotations.Test
import org.testng.annotations.Test
import scala.util.Random

class TestUtil {
  import TestUtil._
  
  @Test
  def checkExpectThatWithExactMatch =
    expectThat(throw new IllegalArgumentException) doesThrow new IllegalArgumentException

  @Test
  def checkExpectThatWithSubclass =
    expectThat(throw new IllegalArgumentException) doesThrow new Exception

  @Test
  def checkExpectThatWithNoExceptionThrowsAssertionError =
    expectThat( expectThat(Unit) doesThrow new Exception ) doesThrow new AssertionError

  @Test
  def checkExpectThatWithIncompatibleExceptionThrowsAssertionError =
    expectThat( expectThat(new IllegalStateException) doesThrow new IllegalArgumentException ) doesThrow new AssertionError
    
}

object TestUtil {
  
  private def getThrowable[T](f: => T): Option[Throwable] = try { f; None } catch { case e => Some(e) }
  
  def expectThat[T](f: => T) = {
    new Object {
      def doesThrow(t: Throwable) =
        getThrowable(f) match {
          case None => throw new AssertionError("expected the code to throw " + t)
          case Some(e) =>
            if (!t.getClass().isInstance(e))
              throw new AssertionError("expected the code to throw " + t + " instead of " + e)
            val messagePattern = t.getMessage match { case null => "" ; case e => e }
            if (!messagePattern.isEmpty && !e.getMessage.matches(messagePattern))
              throw new AssertionError("expected the message pattern <%s>, but got the message <%s>" format (messagePattern, e.getMessage))
        }
    }
  }

  private val random = new Random(0)
  
  def randomString: String = random nextLong() toString
  
}
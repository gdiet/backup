// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.testutils

object Ensure {

  trait WillThrow {
    def willThrow(target: Throwable)
  }
  
  def that[SomeType](function : => SomeType) : WillThrow = new WillThrow {
    override def willThrow(target: Throwable) = {
      try {
        function
        throw new AssertionError("expected throwable not thrown")
      } catch {
        case e if (e.getClass == target.getClass) =>
        case e : AssertionError => throw e
        case e => {
          val error = new AssertionError("thrown %s instead of expected %s".format(e.getClass.getName, target.getClass.getName))
          error.initCause(e)
          throw error
        }
      }
    }
  }
  
}
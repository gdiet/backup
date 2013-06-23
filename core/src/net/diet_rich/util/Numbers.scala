// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

object Numbers {
  
  implicit class OverflowProtectedOps(a: Long) {
    def +#(b: Long): Long = {
      assume(((a+b) compare a) == (math signum b), s"overflow at $a + $b")
      a + b
    }
    def -#(b: Long): Long = +#(-b)
  }
  
}

// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich

package object util {
  
  def nullIsNone[T](t: T): Option[T] = if (t == null) None else Some(t)

  /** Wrapper for Predef.assume with value message parameter instead of 
   *  reference message parameter to enable full branch coverage in tests.
   */
  @annotation.elidable(annotation.elidable.ASSERTION)
  def ASSUME(assumption: Boolean, message: Any) = assume(assumption, message)
  
}
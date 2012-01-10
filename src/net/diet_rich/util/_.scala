// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich

package object util {
  
  def nullIsNone[T](t: T): Option[T] = if (t == null) None else Some(t)
  
}
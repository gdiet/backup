// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

trait NextOptIterator[T] { self : Iterator[T] =>
  def nextOption: Option[T] = if (hasNext) Some(next) else None
}
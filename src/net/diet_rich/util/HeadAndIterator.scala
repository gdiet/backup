// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

trait HeadAndIterator[T] { self : Iterator[T] =>
  def head : T = if (hasNext) next else throw new NoSuchElementException
  def headOption : Option[T] = if (hasNext) Some(next) else None
  def headOnly : Option[T] = {
    val possibleResult = headOption
    if (!hasNext) possibleResult else throw new IllegalStateException("Iterator is not yet empty.")
  }
}
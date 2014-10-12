// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import net.diet_rich.dedup.util.Equal

package object values {
  implicit class RichDataRanges[T <: Traversable[DataRange]](val ranges: T) extends AnyVal {
    def normalize: List[DataRange] = ranges.foldLeft(List.empty[DataRange]) {
      case (Nil, range) => List(range)
      case (head :: tail, range) if head.fin === range.start => DataRange(head.start, range.fin) :: tail
      case (ranges, range) => range :: ranges
    }.reverse
  }
}

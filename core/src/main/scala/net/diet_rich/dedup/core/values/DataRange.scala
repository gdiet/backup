// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.values

case class DataRange(start: Position, fin: Position) extends Ordered[DataRange] {

  def size = fin - start

  def withLength(length: Size) = copy(fin = start + length)

  def withOffset(offset: Size) = copy(start = start + offset)

  def partitionAtBlockLimit(blocksize: Size): (DataRange, Option[DataRange]) =
    if (start.block(blocksize) == fin.block(blocksize))
      (this, None)
    else {
      val newSize = blocksize - start.blockOffset(blocksize)
      (withLength(newSize), Some(withOffset(newSize)))
    }

  def partitionAtSize(size: Size): (DataRange, Option[DataRange]) =
    if (size >= fin-start) (this, None) else (withLength(size), Some(withOffset(size)))

  override def compare(that: DataRange): Int =
    that.start compare start match {
      case 0 => that.fin compare fin
      case x => x
    }
}
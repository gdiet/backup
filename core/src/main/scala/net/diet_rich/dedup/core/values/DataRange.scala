// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.values

case class DataRange(start: Position, fin: Position) {

  def size = fin - start

  def withLength(length: Size) = copy(fin = start + length)

  def withOffset(offset: Size) = copy(start = start + offset)

  private def startBlock(blocksize: Size) = start.value / blocksize.value
  private def finBlock(blocksize: Size) = (fin.value - 1) / blocksize.value
  private def blockOffset(position: Position, blocksize: Size) = Size(position.value % blocksize.value)

  def partitionAtBlockLimit(blocksize: Size): (DataRange, Option[DataRange]) =
    if (startBlock(blocksize) == finBlock(blocksize))
      (this, None)
    else {
      val newSize = blocksize - blockOffset(start, blocksize)
      (withLength(newSize), Some(withOffset(newSize)))
    }

  def partitionAtLimit(limit: Size): (DataRange, Option[DataRange]) =
    if (limit >= size) (this, None) else (withLength(limit), Some(withOffset(limit)))

}

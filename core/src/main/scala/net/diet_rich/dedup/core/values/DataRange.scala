// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.values

case class DataRange(start: Position, fin: Position) {
  import DataRange._

  def size = fin - start

  def withLength(length: Size) = copy(fin = start + length)
  def withOffset(offset: Size) = copy(start = start + offset)
  def shortenBy(length: Size) = copy(fin = fin - size)

  private def startBlock(blocksize: Size) = start.value / blocksize.value
  private def finBlock(blocksize: Size) = (fin.value - 1) / blocksize.value
  private def blockOffset(position: Position, blocksize: Size) = Size(position.value % blocksize.value)

  def limitAt(limit: Size): RangeLimitResult =
    if (size == limit) ExactMatch(this) else
    if (size > limit) IsLarger(withLength(limit), withOffset(limit)) else
    NotLargeEnough(this, limit - size)
}

object DataRange extends ((Position, Position) => DataRange) {
  sealed trait RangeLimitResult
  case class NotLargeEnough(range: DataRange, missing: Size) extends RangeLimitResult
  case class ExactMatch(range: DataRange) extends RangeLimitResult
  case class IsLarger(range: DataRange, rest: DataRange) extends  RangeLimitResult
}
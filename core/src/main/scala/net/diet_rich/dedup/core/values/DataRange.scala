// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.values

final case class DataRange(start: Position, fin: Position) {
  import DataRange._

  def size = fin - start

  def withLength(length: Size) = copy(fin = start + length)
  def withOffset(offset: Size) = copy(start = start + offset)

  def limitAt(limit: Size): RangeLimitResult =
    if (size == limit) ExactMatch(this) else
    if (size > limit) IsLarger(this) else
    NotLargeEnough(this, limit - size)
}

object DataRange extends ((Position, Position) => DataRange) {
  def apply(start: Position, size: Size): DataRange = DataRange(start, start + size)

  sealed trait RangeLimitResult
  final case class NotLargeEnough(range: DataRange, missing: Size) extends RangeLimitResult
  final case class ExactMatch(range: DataRange) extends RangeLimitResult
  final case class IsLarger(range: DataRange) extends  RangeLimitResult
}
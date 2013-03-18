// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.vals

import net.diet_rich.util.Numbers

trait TypedValue[T] {
  val value: T
}

trait ByteArrayValue extends TypedValue[Array[Byte]]

trait LongValue extends TypedValue[Long] {
  def intValue = Numbers.toInt(value)
}

trait OrderedLongValue[Self <: LongValue] extends LongValue with Ordered[Self] { self: Self =>
  override final def compare(other: Self): Int  = value compare other.value
}

trait ApplyCheckedLong[T] {
  def apply(value: Long): T
  def apply(min: => Long)(value: Long): T = {
    assume (min <= value, "value below min")
    apply(value)
  }
  def apply(min: => Long, max: => Long)(value: Long): T = {
    assume (min <= value, "value below min")
    assume (max >= value, "value beyond max")
    apply(value)
  }
}

trait ApplyOption[S, T] {
  def apply(value: S): T
  def apply(value: Option[S]): Option[T] = value.map(apply(_))
}

trait ApplyLongOption[T] extends ApplyOption[Long, T]

case class Size(value: Long) extends OrderedLongValue[Size] {
  def +(other: Size): Size = Size(value + other.value)
  def -(other: Size): Size = Size(value - other.value)
  def asPosition = Position(value)
}
object Size extends ApplyCheckedLong[Size]

case class Position(value: Long) extends OrderedLongValue[Position] {
  def +(other: Size): Position = Position(value + other.value)
  def -(other: Size): Position = Position(value - other.value)
  def -(other: Position): Size = Size(value - other.value)
  def /(other: Size): Long = value / other.value
  def %(other: Size): Size = Size(value / other.value)
}

case class Range(start: Position, end: Position) extends Ordered[Range] {
  assume(end >= start)
  def length = end - start
  def +/ (offset: Size) = copy(start = start + offset)
  override final def compare(that: Range): Int =
    start compare that.start match {
      case 0 => end compare that.end
      case x => x
    }
}

case class Time(value: Long) extends OrderedLongValue[Time]

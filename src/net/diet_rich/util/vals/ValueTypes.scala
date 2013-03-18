// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.vals

trait TypedValue[T] { val value: T }

trait LongValue extends TypedValue[Long]
trait IntValue extends TypedValue[Int]
trait ByteArrayValue extends TypedValue[Array[Byte]]

// FIXME can we use comparable?
trait OrderedLongValue[Self <: LongValue] extends LongValue with Ordered[Self] { self: Self =>
  override final def compare(other: Self): Int  = value compare other.value
}

trait OrderedIntValue[Self <: IntValue] extends IntValue with Ordered[Self] { self: Self =>
  override final def compare(other: Self): Int  = value compare other.value
}

trait ApplyOption[S, T] {
  def apply(value: S): T
  def apply(value: Option[S]): Option[T] = value.map(apply(_))
}

trait ApplyLongOption[T] extends ApplyOption[Long, T]

case class Size(value: Long) extends OrderedLongValue[Size] {
  def +(other: Size): Size = Size(value + other.value)
  def -(other: Size): Size = Size(value - other.value)
}

case class IntSize(value: Int) extends OrderedIntValue[IntSize] {
  def +(other: IntSize): IntSize = IntSize(value + other.value)
  def -(other: IntSize): IntSize = IntSize(value - other.value)
}
object IntSize {
  def checked(value: Long) = {
    assume(value <= Int.MaxValue)
    IntSize(value toInt)
  }
}

case class IntPosition(value: Int) extends OrderedIntValue[IntPosition]

case class Position(value: Long) extends OrderedLongValue[Position] {
  def +(other: Size): Position = Position(value + other.value)
  def +(other: IntSize): Position = Position(value + other.value)
  def -(other: Size): Position = Position(value - other.value)
  def -(other: IntSize): Position = Position(value - other.value)
  def -(other: Position): Size = Size(value - other.value)
  def /(other: IntSize): Long = value / other.value
  def %(other: IntSize): IntSize = IntSize((value % other.value) toInt)
  def %&(other: IntSize): Position = Position(value % other.value)
}

case class Range(start: Position, end: Position) extends Ordered[Range] {
  assume(end >= start)
  def length = end - start
  def +/ (offset: Size) = copy(start = start + offset)
  def +/ (offset: IntSize) = copy(start = start + offset)
  override final def compare(that: Range): Int =
    start compare that.start match {
      case 0 => end compare that.end
      case x => x
    }
}

case class Time(value: Long) extends LongValue

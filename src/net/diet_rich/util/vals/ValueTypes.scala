// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.vals

import java.util.Arrays
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
  protected def checkedAdd(a: Long, b: Long) = {
    val result = a + b
    assume(b > 0 == result > a)
    result
  }
  protected def checkedSub(a: Long, b: Long) = checkedAdd(a, -b)
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
  def +(other: Size): Size = Size(checkedAdd(value, other.value))
  // FIXME use negative "-" as prefix operator instead
  def -(other: Size): Size = Size(checkedSub(value, other.value))
  def asPosition = Position(value)
}
object Size extends ApplyCheckedLong[Size] {
  def min(a: Size, b: Size) = Size(math.min(a.value, b.value))
}

case class Position(value: Long) extends OrderedLongValue[Position] {
  def +(other: Size): Position = Position(checkedAdd(value, other.value))
  def -(other: Size): Position = Position(checkedSub(value, other.value))
  def -(other: Position): Size = Size(checkedSub(value, other.value))
  def /(other: Size): Long = value / other.value
  def %(other: Size): Size = Size(value % other.value)
  def asSize = Size(value)
}
object Position extends ApplyCheckedLong[Position]

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

case class Bytes(bytes: Array[Byte], offset: Position, length: Size) {
  override def equals(a: Any) = a match {
    case null => false
    case Bytes(otherBytes, `offset`, `length`) => Arrays.equals(bytes, otherBytes)
    case _ => false
  }
  override def hashCode() = Arrays.hashCode(bytes) + (length.value + offset.value).toInt
}

case class Time(value: Long) extends OrderedLongValue[Time]

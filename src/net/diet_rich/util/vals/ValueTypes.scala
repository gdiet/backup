// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.vals

trait TypedValue[T] { val value: T }

trait LongValue extends TypedValue[Long]

trait OrderedLongValue[Self <: LongValue] extends LongValue with Ordered[Self] { self: Self =>
  override final def compare(other: Self): Int  = value compare other.value
}

case class Size(value: Long) extends OrderedLongValue[Size] {
  def +(other: Size): Size = Size(value + other.value)
  def -(other: Size): Size = Size(value - other.value)
}

case class Position(value: Long) extends OrderedLongValue[Position] {
  def +(other: Size): Position = Position(value + other.value)
  def -(other: Position): Size = Size(value - other.value)
}

case class Time(value: Long) extends LongValue

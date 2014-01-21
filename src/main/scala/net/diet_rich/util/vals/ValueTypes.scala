// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.vals

case class Size(value: Long) {
  def +  (other: Size): Size     = Size(value + other.value)
  def -  (other: Size): Size     = Size(value - other.value)
  def <  (other: Size): Boolean  = value <  other.value
  def >  (other: Size): Boolean  = value >  other.value
  def <= (other: Size): Boolean  = value <= other.value
  def compare (other: Size): Int = value compare other.value
}

case class Position(value: Long) {
  def +  (other: Size): Position     = Position(value + other.value)
  def -  (other: Position): Size     = Size(value - other.value)
  def >  (other: Position): Boolean  = value >  other.value
  def <  (other: Position): Boolean  = value <  other.value
  def <= (other: Position): Boolean  = value <= other.value
  def compare (other: Position): Int = value compare other.value
  def asSize: Size = Size(value)
}

case class Time(val value: Long)

package net.diet_rich.common.vals

class Position private (val value: Long) extends AnyVal {
  def -(offset: Offset): Position = Position(value - offset.value)
  def +(size: Size): Position = Position(value + size.value)
}
object Position {
  def apply(value: Long): Position = new Position(value)
}

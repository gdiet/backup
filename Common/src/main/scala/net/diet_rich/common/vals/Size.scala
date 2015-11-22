package net.diet_rich.common.vals

// FIXME use
class Size private (val value: Long) extends AnyVal {
  def -(size: IntSize): Size = Size(value - size.value)
  def requirePositive() = require(value > 0, s"Size must be positive but is $value")
}
object Size {
  def apply(value: Long): Size = new Size(value)
}

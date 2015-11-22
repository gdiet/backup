package net.diet_rich.common.vals

class IntSize private(val value: Int) extends AnyVal
object IntSize {
  def apply(value: Int): IntSize = new IntSize(value)
}

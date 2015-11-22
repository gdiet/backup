package net.diet_rich.common.vals

// FIXME rename to match LongSize naming conventions
trait Size32 extends Any { val value: Int }
class IntSize private (val value: Int) extends AnyVal with Size32
object IntSize {
  def apply(value: Int): IntSize = new IntSize(value)
}

package net.diet_rich.common.vals

class Offset private (val value: Long) extends AnyVal
object Offset {
  def apply(value: Long): Offset = new Offset(value)
  def unapply(offset: Offset): Option[Long] = Some(offset.value)
}

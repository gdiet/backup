package net.diet_rich.bytestore.file

// FIXME make part of common if logic is clearly defined and generally usable, like Position and Offset
class FileNumber private (val value: Long) extends AnyVal {
  def *(blocksize: BlockSize): Position = Position(value * blocksize.value)
}
object FileNumber {
  def apply(value: Long): FileNumber = new FileNumber(value)
}

class Position private (val value: Long) extends AnyVal {
  def %(size: BlockSize): Offset = Offset(value % size.value)
  def /(size: BlockSize): FileNumber = FileNumber(value / size.value)
  def -(offset: Offset): Position = Position(value - offset.value)
  def +(size: LongSize): Position = Position(value + size.value)
  def +(size: IntSize): Position = Position(value + size.value)
}
object Position {
  def apply(value: Long): Position = new Position(value)
}

class Offset private (val value: Long) extends AnyVal
object Offset {
  def apply(value: Long): Offset = new Offset(value)
  def unapply(offset: Offset): Option[Long] = Some(offset.value)
}

trait LongSize extends Any { val value: Long }
class Size private (val value: Long) extends AnyVal with LongSize {
  def -(size: IntSize): Size = Size(value - size.value)
  def requirePositive() = require(value > 0, s"Size must be positive but is $value")
}
object Size {
  def apply(value: Long): Size = new Size(value)
}

class BlockSize private(val value: Long) extends AnyVal with LongSize {
  def -(offset: Offset): Size = Size(value - offset.value)
}
object BlockSize {
  def apply(value: Long): BlockSize = new BlockSize(value)
}

class IntSize private (val value: Int) extends AnyVal
object IntSize {
  def apply(value: Int): IntSize = new IntSize(value)
}

package net.diet_rich.bytestore.file

import net.diet_rich.common.vals._

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

class BlockSize private(val value: Long) extends AnyVal with LongSize {
  def -(offset: Offset): Size = Size(value - offset.value)
}
object BlockSize {
  def apply(value: Long): BlockSize = new BlockSize(value)
}

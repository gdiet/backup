package net.diet_rich.bytestore

import net.diet_rich.common.vals._

package object file {
  /** Note: This function is designed only for positive values. */
  def min(s1: IntSize, s2: Size, s3: Size): IntSize = IntSize(math.min(s1.value, math.min(s2.value, s3.value)).toInt)
  /** Note: This function is designed only for positive values. */
  def min(s1: IntSize, s2: Long): Int = math.min(s1.value, s2).toInt

  def printOf(string: String): Print = Print of string

  class FileNumber private (val value: Long) extends AnyVal {
    def *(blocksize: BlockSize): Position = Position(value * blocksize.value)
  }
  object FileNumber { def apply(value: Long): FileNumber = new FileNumber(value) }

  class BlockSize private(val value: Long) extends AnyVal {
    def -(offset: Offset): Size = Size(value - offset.value)
  }
  object BlockSize { def apply(value: Long): BlockSize = new BlockSize(value) }

  implicit class RichPosition(val position: Position) {
    def +(blockSize: BlockSize): Position = Position(position.value + blockSize.value)
    def +(size: IntSize): Position = Position(position.value + size.value)
    def %(size: BlockSize): Offset = Offset(position.value % size.value)
    def /(size: BlockSize): FileNumber = FileNumber(position.value / size.value)
  }
}

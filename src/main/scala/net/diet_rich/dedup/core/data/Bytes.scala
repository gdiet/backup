package net.diet_rich.dedup.core.data

case class Bytes (data: Array[Byte], offset: Int, length: Int) {
  def addOffset(offset: Int) = copy(data, this.offset + offset, length - offset)
}

object Bytes extends ((Array[Byte], Int, Int) => Bytes) {
  val empty = Bytes(Array.empty, 0, 0)
  def zero(length: Int) = Bytes(new Array(length), 0, length)
}

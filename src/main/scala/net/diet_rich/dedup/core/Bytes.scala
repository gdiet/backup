package net.diet_rich.dedup.core

case class Bytes (data: Array[Byte], offset: Int, length: Int) {
  def addOffset(offset: Int) = copy(data, this.offset + offset, length - offset)
  def setLength(length: Int) = copy(data, offset, length)
}

object Bytes extends ((Array[Byte], Int, Int) => Bytes) {
  def zero(length: Int) = Bytes(new Array(length), 0, length)
}

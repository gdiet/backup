package net.diet_rich.dedup.core.data

import net.diet_rich.dedup.util._

import scala.collection.mutable.MutableList

case class Bytes (data: Array[Byte], offset: Int, length: Int) {
  def addOffset(offset: Int) = copy(data, this.offset + offset, length - offset)
}

object Bytes extends ((Array[Byte], Int, Int) => Bytes) {
  val empty = Bytes(Array.empty, 0, 0)
  def zero(length: Int) = Bytes(new Array(length), 0, length)

  // Note: MutableList allows in-place replacement when applying the store method to minimize memory impact
  def consumingIterator(data: MutableList[Bytes]): Iterator[Bytes] = new Iterator[Bytes] {
    var index = 0
    def hasNext: Boolean = index < data.size
    def next: Bytes = valueOf(data(index)) before {
      data.update(index, empty)
      index += 1
    }
  }
}

package net.diet_rich.common

import java.util

case class Bytes (data: Array[Byte], offset: Int, length: Int) {
  require(length >= 0, s"negative length: $length")
  require(offset >= 0, s"negative offset: $offset")
  require(data.length - offset >= length, s"not enough data (${data.length} bytes) for length $length at offset $offset")
  def withOffset(offset: Int) = copy(data, this.offset + offset, length - offset)
  def withLength(length: Int) = copy(length = length)
  def asByteArray = data slice (offset, offset+length)
  override def equals(other: Any): Boolean = other match {
    case other @ Bytes(_, _, `length`) => util.Arrays equals (asByteArray, other.asByteArray)
    case _ => false
  }
  override def toString =
    if (length <= 10) s"[${asByteArray.mkString(",")}]"
    else s"Bytes(Byte[${data.length}],$offset,$length)"
}

object Bytes extends ((Array[Byte], Int, Int) => Bytes) {
  def apply(data: Array[Byte]): Bytes = Bytes(data, 0, data.length)
  val empty = Bytes(Array.empty, 0, 0)
  def zero(length: Int) = Bytes(new Array(length), 0, length)
}

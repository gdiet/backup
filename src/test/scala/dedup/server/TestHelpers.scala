package dedup
package server

extension(long: Long) def asInt: Int =
  require(long >= 0 && long <= Int.MaxValue, s"Illegal int+ value: $long")
  long.toInt

given DataSink[Array[Byte]] with
  extension(d: Array[Byte]) def write(offset: Long, data: Array[Byte]): Unit =
    System.arraycopy(data, 0, d, offset.asInt, data.length)

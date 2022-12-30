package dedup
package server

extension(long: Long) def asInt: Int =
  require(long >= 0 && long <= Int.MaxValue, s"Illegal int+ value: $long")
  long.toInt

extension(backend: Backend)
  def testRead(fileId: Long, offset: Long, requestedSize: Long) =
    val buffer = new Array[Byte](requestedSize.asInt)
    val sizeRead = backend.read(fileId, offset, requestedSize).get.foldLeft(0) { case (size, position -> data) =>
      System.arraycopy(data, 0, buffer, (position - offset).asInt, data.length)
      size + data.length
    }
    assert(sizeRead <= requestedSize)
    buffer.take(sizeRead)

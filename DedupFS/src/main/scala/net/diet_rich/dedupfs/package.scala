package net.diet_rich

import java.io.InputStream

import net.diet_rich.common._

package object dedupfs {

  implicit class BytesIteratorToInputStream(val source: Iterator[Bytes]) extends AnyVal {
    def asInputStream: InputStream = {
      new InputStream {
        var current: Option[Bytes] = None
        def refill() = current = if (source.hasNext) Some(source.next()) else None
        refill()
        override def read(target: Array[Byte], targetOffset: Int, targetLength: Int): Int = current match {
          case None => -1
          case Some(bytes @ Bytes(data, offset, length)) =>
            val actualLength = math.min(length, targetLength)
            System.arraycopy(data, offset, target, targetOffset, actualLength)
            if (actualLength == length) refill() else current = Some(bytes addOffset actualLength)
            actualLength
        }
        override def read: Int = current match {
          case None => -1
          case Some(bytes @ Bytes(data, offset, length)) =>
            if (length == 1) refill() else current = Some(bytes addOffset 1)
            data(offset)
        }
      }
    }
  }

}

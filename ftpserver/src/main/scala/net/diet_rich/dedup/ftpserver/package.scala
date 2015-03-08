package net.diet_rich.dedup

import java.io.{InputStream, IOException}

import net.diet_rich.dedup.core.data.Bytes

package object ftpserver {
  implicit class BytesIteratorToInputStream(val data: Iterator[Bytes]) extends AnyVal {
    def asInputStream: InputStream = {
      new InputStream {
        var current: Option[Bytes] = None
        def refill() = current = if (data.hasNext) Some(data.next()) else None
        refill()
        override def read(target: Array[Byte], offset: Int, length: Int): Int = current match {
          case None => -1
          case Some(bytes) =>
            val actualLength = math.min(bytes.length, length)
            System.arraycopy(bytes.data, bytes.offset, target, offset, actualLength)
            if (actualLength == bytes.length) refill() else current = Some(bytes addOffset actualLength)
            actualLength
        }
        override def read: Int = {
          val array = new Array[Byte](1)
          read(array, 0, 1) match {
            case -1 => -1
            case  1 => array(0)
          }
        }
      }
    }
  }
}

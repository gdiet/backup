// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup

import java.io.{InputStream, IOException}

import net.diet_rich.dedup.core.values.Bytes

package object ftpserver {

  implicit class EnhancesBytesIterator(val data: Iterator[Bytes]) extends AnyVal {
    def asInputStream: InputStream = {
      new InputStream {
        var currentOffset: Int = 0
        var currentLength: Int = 0
        var currentBytes: Array[Byte] = Array()
        def refill() = if (data.hasNext) {
          val current = data.next()
          currentBytes = current.data
          currentOffset = current.offset
          currentLength = current.length
        } else currentLength = 0
        refill()
        override def read(bytes: Array[Byte], offset: Int, length: Int): Int = if (currentLength == 0) -1 else {
          val actualLength = math.min(currentLength, length)
          System.arraycopy(currentBytes, currentOffset, bytes, offset, actualLength)
          if (currentLength == actualLength) refill() else {
            currentLength -= actualLength
            currentOffset += actualLength
          }
          actualLength
        }
        override def read: Int = {
          val array = new Array[Byte](1)
          read(array, 0, 1) match {
            case n if n < 1 => -1
            case 1 => array(0)
            case n => throw new IOException(s"unexpected number of bytes read: $n")
          }
        }
      }
    }
  }

}

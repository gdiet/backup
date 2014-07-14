// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import java.io.RandomAccessFile

package object values {

  implicit class BytesReader(val bytes: Bytes) extends AnyVal {
    def fillFrom(input: RandomAccessFile): Bytes = {
      import bytes._
      @annotation.tailrec
      def readRecurse(offset: Int, length: Int): Int =
        input.read(data, offset, length) match {
          case n if n < 1 => offset
          case n => if (n == length) offset + n else readRecurse(offset + n, length - n)
        }
      withSize(readRecurse(offset, length) - offset)
    }
  }

}

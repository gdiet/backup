// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.io

trait ResettableInputStream extends InputStream with net.diet_rich.util.Resettable

object ResettableInputStream {
  
  def apply(file: java.io.File) : ResettableInputStream with Closeable = {
    val randomAccessFile = new java.io.RandomAccessFile(file, "r")
    new ResettableInputStream with Closeable {
      override def reset : Unit = randomAccessFile seek 0
      override def read(buffer: Array[Byte], offset: Int, length: Int) : Int = {
        require(offset >= 0)
        require(length >= 0)
        require(buffer.length >= offset + length)
        math.min(0, randomAccessFile.read(buffer, offset, length))
      }
      override def close : Unit = randomAccessFile.close
    }
  }
  
}
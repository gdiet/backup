// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.io

import java.io.File
import net.diet_rich.util.Bytes

trait RandomAccessInputStream extends InputStream with Seekable

trait RandomAccessStream extends RandomAccessInputStream with OutputStream

class RandomAccessFileInput(file: File) extends RandomAccessInputStream {
  protected val randomAccessFile = new java.io.RandomAccessFile(file, "rw")
  override def seek(position: Long) : Unit = randomAccessFile seek position
  override def close() : Unit = randomAccessFile.close
  override def read(bytes: Bytes) : Int = {
    randomAccessFile.read(bytes.bytes, bytes.offset, bytes.length) match {
      case x if x < 0 => 0
      case x => x
    }
  }
}

class RandomAccessFile(file: File) extends RandomAccessFileInput(file) with RandomAccessStream {
  override def write(bytes: Bytes) : Unit = randomAccessFile.write(bytes.bytes, bytes.offset, bytes.length)
}

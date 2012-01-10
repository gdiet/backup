// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.io

import java.io.File
import net.diet_rich.util.data.Bytes

/** Note: The file size and content may change while the input is open. */
class RandomAccessFileInput(file: File) extends RandomAccessInput {
  protected val accessMode = "r"
  protected final lazy val randomAccessFile = new java.io.RandomAccessFile(file, accessMode)
  override final def length : Long = randomAccessFile length
  override final def seek(position: Long) : Unit = randomAccessFile seek position
  override final def close() : Unit = randomAccessFile.close
  override final def read(bytes: Bytes) : Bytes =
    bytes.keepFirst(math.max(0, randomAccessFile.read(bytes.bytes, bytes.offset, bytes.length)))
}

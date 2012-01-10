// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.io

import java.io.File
import net.diet_rich.util.Bytes

class RandomAccessFile(file: File) extends RandomAccessFileInput(file) with OutputStream {
  override protected val accessMode = "rw"
  override final def write(bytes: Bytes) = { randomAccessFile.write(bytes.bytes, bytes.offset, bytes.length) ; this }
}

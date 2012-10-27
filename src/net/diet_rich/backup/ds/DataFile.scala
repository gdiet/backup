// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.ds

import java.io.File
import net.diet_rich.util.ImmutableBytes
import net.diet_rich.util.io._
import java.io.RandomAccessFile

// FIXME no separate class needed - inline the code?
class DataFile(val dataLength: Int, val file: File) {
  assume (dataLength > 0, "data length of file %s must be positive but is %s" format (file, dataLength))
  assume (!file.exists || file.isFile, "data file %s must be a regular file if it exists" format file)
  
  def readAndPadZeros(off: Int = 0, len: Int = dataLength): (Int, Array[Byte]) = {
    assume (file.isFile, "data file %s must be an existing regular file" format file)
    assume (len > 0, "data length to read from file %s must be positive but is %s" format (file, len))
    assume (off + len <= dataLength, "offset %s + length %s must be less or equal dataLength %s when reading from file %s" format (off, len, dataLength, file))
    
    val bytes = new Array[Byte](len)
    (using(new RandomAccessFile(file, "r")){ fillFrom(_, bytes, off, len) }, bytes)
  }

  def writeFully(bytes: Array[Byte]) : Unit = {
    assume (bytes.length <= dataLength, "array size %s must be less or equal the data file length %s for file %s" format (bytes.length, dataLength, file))
    
    using(new RandomAccessFile(file, "rw")){ _ write (bytes) }
  }

}
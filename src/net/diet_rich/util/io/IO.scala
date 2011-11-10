// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.io

import java.io.RandomAccessFile
import net.diet_rich.util.Bytes

object IO {

  type ByteInput = { def read (bytes: Array[Byte], offset: Int, length: Int) : Int }

  // FIXME not needed anymore
  
  /** Blocks until requested length is read or end of stream is reached.
   * 
   *  @return The number of bytes read.
   */
  def readFromByteInput(byteInput: ByteInput, bytes: Bytes) : Int = {
    @annotation.tailrec
    def readBytesTailRec(bytes: Bytes) : Int =
      byteInput.read(bytes.bytes, bytes.offset, bytes.length) match {
        case bytesRead if bytesRead <= 0 => bytes.length
        case bytesRead => readBytesTailRec(bytes.dropFirst(bytesRead))
      }
    bytes.length - readBytesTailRec(bytes)
  }
  
  /** Blocks until requested length is read or end of stream is reached.
   * 
   *  @return The bytes read.
   */
  def readFromByteInput(byteInput: ByteInput, length: Int) : Bytes = {
    require(length >= 0)
    val bytes = Bytes(length)
    bytes.copy(length = readFromByteInput(byteInput, bytes))
  }
  
}
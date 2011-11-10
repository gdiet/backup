// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.io

import net.diet_rich.util.Bytes

trait InputStream extends Closeable {

  /** If length is not zero, blocks until at least one byte is read 
   *  or end of stream is reached.
   * 
   *  @return The number of bytes read.
   */
  def read(bytes: Bytes) : Int

  /** If length is not zero, blocks until at least one byte is read 
   *  or end of stream is reached.
   */
  def read(length: Int) : Bytes = {
    val bytes = Bytes(length)
    bytes.copy(length = read(bytes))
  }
  
  /** Blocks until requested length is read or end of stream is reached.
   * 
   *  @return The number of bytes read.
   */
  def readFully(bytes: Bytes) : Int = {
    @annotation.tailrec
    def readBytesTailRec(bytes: Bytes) : Int =
      read(bytes) match {
        case bytesRead if bytesRead <= 0 => bytes.length
        case bytesRead                   => readBytesTailRec(bytes.dropFirst(bytesRead))
      }
    bytes.length - readBytesTailRec(bytes)
  }
  
  /** Blocks until requested length is read or end of stream is reached. */
  def readFully(length: Int) : Bytes = {
    val bytes = Bytes(length)
    bytes.copy(length = readFully(bytes))
  }
  
  /** The default close implementation does nothing at all. */
  override def close : Unit = Unit

}

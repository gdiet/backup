// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.io

import java.io.Closeable
import net.diet_rich.util.data.Bytes

trait InputStream extends Closeable {

  /** If length is not zero, blocks until at least one byte is read 
   *  or end of stream is reached.
   */
  def read(bytes: Bytes) : Bytes

  /** If length is not zero, blocks until at least one byte is read 
   *  or end of stream is reached.
   */
  def read(length: Int) : Bytes = read(Bytes(length))
  
  /** Blocks until requested length is read or end of stream is reached.
   */
  def readFully(bytes: Bytes) : Bytes = {
    @annotation.tailrec
    def readBytesTailRec(bytes: Bytes) : Int =
      read(bytes) length match {
        case 0 => bytes length
        case length => readBytesTailRec(bytes dropFirst length)
      }
    bytes.keepFirst(bytes.length - readBytesTailRec(bytes))
  }
  
  /** Blocks until requested length is read or end of stream is reached. */
  def readFully(length: Int) : Bytes = readFully(Bytes(length))
  
  def copyTo(output: OutputStream) : Long = {
    val bytes = Bytes(32768)
    @annotation.tailrec
    def copyBytesTailRec : Long =
      read(bytes).length match {
        case 0 => 0
        case length =>
          output write bytes
          bytes extend()
          copyBytesTailRec
      }
    
    0L
  }
  
  /** The default close implementation does nothing at all. */
  override def close : Unit = Unit

}

object InputStream {
  class Empty extends InputStream {
    override def read(bytes: Bytes) : Bytes = bytes.copy(length = 0)
  }
  val empty = new Empty
}


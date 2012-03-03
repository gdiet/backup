// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.io

import java.io.Closeable
import net.diet_rich.util.data.Bytes

trait InputStream extends Closeable {

  /** Read into provided bytes object. If length is not zero, blocks 
   *  until at least one byte is read or end of stream is reached.
   */
  protected def read(bytes: Bytes) : Bytes

  /** If length is not zero, blocks until at least one byte is read 
   *  or end of stream is reached. The returned Bytes can be 0-padded
   *  extended up to 'length'.
   */
  def read(length: Int) : Bytes = read(Bytes(length))

  def readLong : Long = read(8) toLong
  def readExtendLong : Long = read(8) extend 8 longFrom
  
  /** Blocks until requested length is read or end of stream is reached.
   */
  def readFully(length: Int) : Bytes = {
    @annotation.tailrec
    def readBytesTailRec(bytes: Bytes) : Int =
      read(bytes) length match {
        case 0 => bytes length
        case length => readBytesTailRec(bytes dropFirst length)
      }
    val result = Bytes(length)
    result.keepFirst(length - readBytesTailRec(result))
  }
  
  def copyTo(output: OutputStream) : Long = {
    val bytes = Bytes(32768)
    @annotation.tailrec
    def copyBytesTailRec : Long =
      read(bytes).length match {
        case 0 => 0
        case length =>
          output write bytes
          bytes.extendMax
          copyBytesTailRec
      }
    
    0L
  }
  
  /** The default close implementation does nothing at all. */
  override def close : Unit = Unit

}

object InputStream {
  trait Empty extends InputStream {
    override protected def read(bytes: Bytes) : Bytes = bytes.keepFirst(0)
  }
  val empty = new Empty {}
}


// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.io

trait RandomAccessInput extends InputStream {
  /** Sets the data offset, measured from the start of data,
   *  at which the next data operation occurs. The offset may
   *  be set beyond the end of the data.
   */
  def seek(position: Long) : Unit
  def length : Long
}

object RandomAccessInput {
  val empty = new InputStream.Empty with RandomAccessInput {
    override def length = 0
    override def seek(position: Long) = Unit
  }
}
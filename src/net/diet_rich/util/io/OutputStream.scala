// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.io

trait OutputStream {

  def write(buffer: Array[Byte], offset: Int, length: Int) : Unit
  
}

object OutputStream {
  
  val empty = new OutputStream {
    override def write(buffer: Array[Byte], offset: Int, length: Int) : Unit = { }
  }
  
}
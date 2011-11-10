// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.io

import net.diet_rich.util.Bytes

trait OutputStream {
  def write(bytes: Bytes) : Unit
}

object OutputStream {
  val empty = new OutputStream {
    override def write(bytes: Bytes) : Unit = { }
  }
}

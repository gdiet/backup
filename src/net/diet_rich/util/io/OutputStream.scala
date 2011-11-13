// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.io

import net.diet_rich.util.Bytes

trait OutputStream[+Repr] { def write(bytes: Bytes) : Repr }

object OutputStream {
  val empty = new OutputStream[Any] {
    override def write(bytes: Bytes) = this
  }
  
  def tee(out1: OutputStream[Any], out2: OutputStream[Any]) = new OutputStream[Any] {
    override def write(bytes: Bytes) = { out1.write(bytes) ; out2.write(bytes) ; this }
  }
}

// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.io

import net.diet_rich.util.Bytes

trait OutputStream extends Closeable { def write(bytes: Bytes) : Unit }

object OutputStream {
  val empty = new OutputStream {
    override def write(bytes: Bytes) = this
    override def close = Unit
  }

  def tee(out1: OutputStream, out2: OutputStream) = new OutputStream {
    override def write(bytes: Bytes) = { out1.write(bytes) ; out2.write(bytes) }
    override def close = try { out1.close } finally { out2.close }
  }
}

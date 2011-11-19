// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.io

import net.diet_rich.util.Bytes

trait OutputStream[+Repr] extends Closeable { def write(bytes: Bytes) : Repr }

trait BasicOutputStream extends OutputStream[BasicOutputStream]

object OutputStream {
  val empty = new BasicOutputStream {
    override def write(bytes: Bytes) : BasicOutputStream = this
    override def close = Unit
  }

  def tee(out1: OutputStream[Any], out2: OutputStream[Any]) = new OutputStream[Any] {
    override def write(bytes: Bytes) = { out1.write(bytes) ; out2.write(bytes) ; this }
    override def close = try { out1.close } finally { out2.close }
  }
}

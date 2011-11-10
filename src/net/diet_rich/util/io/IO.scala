// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.io

import net.diet_rich.util.Bytes

trait Closeable {
  def close() : Unit
}

trait Seekable {
  def seek(position: Long) : Unit
}

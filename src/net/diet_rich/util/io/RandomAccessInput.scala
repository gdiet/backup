// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.io

trait RandomAccessInput extends InputStream {
  def seek(position: Long) : Unit
  def length : Long
}

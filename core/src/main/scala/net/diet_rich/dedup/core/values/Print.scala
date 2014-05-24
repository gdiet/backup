// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.values

import net.diet_rich.dedup.util.{Bytes, init}

case class Print(val value: Long) extends LongValue

object Print extends (Long => Print) {
  import java.util.zip.{Adler32, CRC32}
  def apply(bytes: Bytes): Print =
    Print(init(new Adler32){_ update bytes}.getValue << 32 | init(new CRC32){_ update bytes}.getValue)
}

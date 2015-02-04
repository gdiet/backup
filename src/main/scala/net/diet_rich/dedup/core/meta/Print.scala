package net.diet_rich.dedup.core.meta

import java.util.zip.{CRC32, Adler32}

import net.diet_rich.dedup.core.Bytes
import net.diet_rich.dedup.util._

object Print {
  val empty = Print(Bytes.empty)
  def apply(bytes: Bytes): Long =
    init(new Adler32){_ update (bytes.data, bytes.offset, bytes.length)}.getValue << 32 |
    init(new CRC32  ){_ update (bytes.data, bytes.offset, bytes.length)}.getValue
}

package net.diet_rich.dedup.core.data

import java.util.zip.{Adler32, CRC32}

import net.diet_rich.dedup.util._

object Print {
  val empty = Print(Bytes.empty)
  def apply(bytes: Bytes): Long =
    init(new Adler32){_ update (bytes.data, bytes.offset, bytes.length)}.getValue << 32 |
    init(new CRC32  ){_ update (bytes.data, bytes.offset, bytes.length)}.getValue
}

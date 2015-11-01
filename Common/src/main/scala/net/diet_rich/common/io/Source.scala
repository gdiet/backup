package net.diet_rich.common.io

import net.diet_rich.common._

object Source {
  val readBlockSize = 0x8000
}

trait Source { import Source._
  /** Reads up to (but not necessarily exactly) count bytes. */
  def read(count: Int): Bytes
  final def allData: Iterator[Bytes] = new Iterator[Bytes] {
    var currentBytes = read(readBlockSize)
    override def hasNext = currentBytes.length > 0
    override def next() = valueOf(currentBytes) before {currentBytes = read(readBlockSize)}
  }
}

trait SizedSource extends Source {
  def size: Long
}

trait FileLikeSource extends SizedSource with AutoCloseable {
  def reset(): Unit
}

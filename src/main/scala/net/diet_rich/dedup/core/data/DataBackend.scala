package net.diet_rich.dedup.core.data

import net.diet_rich.dedup.core.StartFin

trait DataBackend {
  /** @return the current position if at a block start, else the position of the next block start */
  def nextBlockStart(position: Long): Long
  def read(range: StartFin): Iterator[Bytes]
  def write(data: Bytes, start: Long): Unit
  def close(): Unit
}

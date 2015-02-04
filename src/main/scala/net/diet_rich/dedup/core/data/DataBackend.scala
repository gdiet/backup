package net.diet_rich.dedup.core.data

import java.io.File

import net.diet_rich.dedup.core.Bytes

trait DataBackend {
  def blockAligned(position: Long): Long
  def read(start: Long, size: Long): Iterator[Bytes]
  def write(data: Bytes, start: Long): Unit
  def close(): Unit
}

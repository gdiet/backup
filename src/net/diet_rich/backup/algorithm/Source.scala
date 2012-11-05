package net.diet_rich.backup.algorithm

import net.diet_rich.util.io.SeekReader

trait SourceEntry {
  def hasData: Boolean
  def name: String
  def time: Long
  def size: Long
  def children: Iterable[SourceEntry]
  /** implementations are responsible for closing the reader. */
  def read[ReturnType]: (SeekReader => ReturnType) => ReturnType
}

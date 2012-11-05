package net.diet_rich.backup.alg

import net.diet_rich.util.io.SeekReader

trait SourceEntry {
  def hasData: Boolean
  def name: String
  def time: Long
  def size: Long
  def children: Iterable[SourceEntry]
  def read[ReturnType]: (SeekReader => ReturnType) => ReturnType
}

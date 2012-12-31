// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.backup

import net.diet_rich.util.io._
import net.diet_rich.util.vals._

trait TreeSource[+Repr] {
  def hasData: Boolean
  def name: String
  def time: Time
  def size: Size
  def children: Iterable[Repr]
  def reader: SeekReader
}

class FileSource(val file: java.io.File) extends TreeSource[FileSource] {
  def hasData: Boolean = file.isFile
  def name: String = file.getName
  def time: Time = Time(file.lastModified)
  def size: Size = Size(file.length)
  def children: Iterable[FileSource] = Nil
  def reader: SeekReader =
    if (hasData) new java.io.RandomAccessFile(file, "r") else emptyReader
}

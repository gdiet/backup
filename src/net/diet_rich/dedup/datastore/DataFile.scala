// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.datastore

import java.io._
import net.diet_rich.util._
import net.diet_rich.util.io.fillFrom
import net.diet_rich.util.vals._

class DataFile(position: Position, val path: File) {

  private var usageCount = 0
  private var file: Option[RandomAccessFile] = None
  private var readonly = true

  def acquire = synchronized {
    assume(usageCount >= 0)
    usageCount = usageCount + 1
  }

  def release = synchronized {
    assume(usageCount >= 1)
    usageCount = usageCount - 1
  }

  private def initializeFileReadWrite = {
    assume(readonly || file.isEmpty)
    file.foreach(_.close) // if open readonly
    path.getParentFile.mkdirs
    val alreadyExisted = path.isFile
    val result = new RandomAccessFile(path, "rw")
    if (!alreadyExisted)
      result.write(position.value.toByteArray)
    file = Some(result)
    result
  }

  private def initializeFileReadOnly = {
    assume(file.isEmpty)
    file = Some(new RandomAccessFile(path, "r"))
    file.get
  }
  
  def write(position: Long, bytes: Array[Byte], offset: Position, size: Size) = synchronized {
    assume(usageCount >= 1)
    assume(offset.value <= Int.MaxValue)
    assume(size.value <= Int.MaxValue)
    val out = file.getOrElse(initializeFileReadWrite)
    out.seek(position)
    out.write(bytes, offset.value toInt, size.value toInt)
  }

  def read(position: Long, bytes: Array[Byte], offset: Position, size: Size): Int = synchronized {
    assume(usageCount >= 1)
    assume(offset.value <= Int.MaxValue)
    assume(size.value <= Int.MaxValue)
    val in = file.getOrElse(initializeFileReadOnly)
    in.seek(position)
    fillFrom(in, bytes, offset.value toInt, size.value toInt)
  }

  def closeIfUnused = synchronized {
    assume(usageCount == 0)
    if (!isInUse) close
  }
  
  def close = synchronized {
    assume(usageCount == 0)
    file.foreach(_.close)
    file = None
  }

  def isInUse = synchronized {
    assume(usageCount >= 0)
    usageCount > 0
  }
  
  def isClosedAndUnused = synchronized {
    assume(usageCount >= 0)
    file.isEmpty && usageCount == 0
  }
}

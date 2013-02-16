// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.datastore

import java.io._
import net.diet_rich.util.io.fillFrom
import net.diet_rich.util.vals._

class DataFile2(val position: Position, val file: java.io.File) extends Closeable {

  protected final def maybeFileAccess = maybeFileAccessor 
  protected final def usageCount = usageCounter
  
  private final var maybeFileAccessor: Option[RandomAccessFile] = None
  private var usageCounter = 0

  // is overridden in DataFileWrite
  protected def internalInitializeFile = (new RandomAccessFile(file, "r"), true)
  
  protected final def initializeFile = {
    assume(maybeFileAccess.isEmpty)
    assume(usageCount == 1)
    val (fileAccessor, mayCheckHeader) = internalInitializeFile
    if (mayCheckHeader) assume(fileAccessor.readLong() == position.value)
    maybeFileAccessor = Some(fileAccessor)
    fileAccessor
  }
  
  final def acquire = synchronized {
    assume(usageCount >= 0)
    usageCounter = usageCounter + 1
  }

  final def release = synchronized {
    assume(usageCount >= 1)
    usageCounter = usageCounter - 1
  }
  
  // is overridden in DataFileWrite
  override def close = synchronized {
    assume(usageCount == 0)
    maybeFileAccess.foreach(_.close)
    maybeFileAccessor = None
  }

  final def closeIfUnused = synchronized {
    assume(usageCount >= 0)
    if (usageCount == 0) close
  }
  
  final def isClosedAndUnused = synchronized {
    assume(usageCount >= 0)
    maybeFileAccess.isEmpty && usageCount == 0
  }
}

object DataFile2 {
  val headerBytes = 16
}

trait DataFileRead {
  self: DataFile2 =>
  final def read(position: Long, bytes: Array[Byte], offset: Position, size: Size): Int = synchronized {
    assume(usageCount >= 1)
    assume(offset.value <= Int.MaxValue)
    assume(size.value <= Int.MaxValue)
    val randomAccessFile = maybeFileAccess.getOrElse(initializeFile)
    randomAccessFile.seek(position + DataFile2.headerBytes)
    fillFrom(randomAccessFile, bytes, offset.value toInt, size.value toInt)
  }
}

trait DataFileWrite extends Closeable {
  self: DataFile2 =>
  
  private var dataPrint: Long = 0

  abstract final override def close = synchronized {
    maybeFileAccess.foreach { fileAccessor =>
      fileAccessor.seek(8)
      fileAccessor.writeLong(dataPrint)
    }
    super.close
  }
  
  override final protected def internalInitializeFile = {
    val alreadyExisted = file.isFile
    if (!alreadyExisted) file.getParentFile.mkdirs
    val fileAccessor = new RandomAccessFile(file, "rw")
    if (!alreadyExisted) fileAccessor.writeLong(position.value)
    (fileAccessor, alreadyExisted)
  }

  final def write(position: Long, bytes: Array[Byte], offset: Position, size: Size): Unit = synchronized {
    assume(usageCount >= 1)
    assume(offset.value <= Int.MaxValue)
    assume(size.value <= Int.MaxValue)
    val randomAccessFile = maybeFileAccess.getOrElse(initializeFile)
    randomAccessFile.seek(position + DataFile2.headerBytes)
    randomAccessFile.write(bytes, offset.value toInt, size.value toInt)
  }
}

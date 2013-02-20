// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.datastore

import java.io._
import net.diet_rich.util.io.fillFrom
import net.diet_rich.util.vals._
import DataFile2.headerBytes

// FIXME rename
object DataFile2 {
  val headerBytes = 16
}

class DataFile2(protected val position: Position, protected val file: File) {

  protected final def randomAccessFile: RandomAccessFile = maybeFileAccess.getOrElse(initializeFile)
  protected final def maybeFileAccess: Option[RandomAccessFile] = maybeFileAccessor 
  protected final def usageCount: Int = usageCounter
  protected final def dataPrint: Long = dataPrintVar
  
  private var maybeFileAccessor: Option[RandomAccessFile] = None
  private var usageCounter: Int = 0
  private var dataPrintVar: Long = 0

  // is overridden in DataFileWrite
  protected def internalInitializeFile = new RandomAccessFile(file, "r")
  
  protected final def initializeFile = {
    assume(maybeFileAccess.isEmpty)
    assume(usageCount == 1)
    val randomAccessFile = internalInitializeFile
    randomAccessFile.seek(0)
    dataPrintVar = randomAccessFile.readLong()
    assume (randomAccessFile.readLong() == position.value)
    maybeFileAccessor = Some(randomAccessFile)
    randomAccessFile
  }

  final def isDataPrintOK: Boolean = synchronized ({
    assume(usageCount >= 1)
    val bytes = new Array[Byte](randomAccessFile.length().toInt - headerBytes)
    randomAccessFile.seek(headerBytes)
    fillFrom(randomAccessFile, bytes, 0, bytes.length)
    calcDataPrint(dataPrint, bytes, 0, bytes.length) == 0
  })
  
  protected final def calcDataPrint(seed: Long, bytes: Array[Byte], offset: Int, size: Int): Long = {
    var print = seed
    for (n <- offset until offset + size)
      print = print ^ (n + 1) * 5870203405204807807L * bytes(n)
    print
  }
  
  protected final def updateDataPrint(bytes: Array[Byte], offset: Int, size: Int) =
    dataPrintVar = calcDataPrint(dataPrint, bytes, offset, size)
  
  final def acquire = synchronized {
    assume(usageCount >= 0)
    usageCounter = usageCounter + 1
  }

  final def release = synchronized {
    assume(usageCount >= 1)
    usageCounter = usageCounter - 1
  }
  
  // is overridden in DataFileWrite
  def close = synchronized {
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

trait DataFileRead extends DataFile2 {
  final def read(position: Long, bytes: Array[Byte], offset: Position, size: Size): Int = synchronized {
    assume(usageCount >= 1)
    assume(offset.value <= Int.MaxValue)
    assume(size.value <= Int.MaxValue)
    randomAccessFile.seek(position + headerBytes)
    fillFrom(randomAccessFile, bytes, offset.value toInt, size.value toInt)
  }
}

trait DataFileWrite extends DataFile2 {
  final override def close = synchronized {
    maybeFileAccess.foreach { fileAccessor =>
      fileAccessor.seek(0)
      fileAccessor.writeLong(dataPrint)
    }
    super.close
  }
  
  override final protected def internalInitializeFile = {
    val alreadyExisted = file.isFile
    if (!alreadyExisted) file.getParentFile.mkdirs
    val fileAccessor = new RandomAccessFile(file, "rw")
    fileAccessor.seek(8)
    if (!alreadyExisted) fileAccessor.writeLong(position.value)
    fileAccessor
  }

  final def writeNewData(position: Long, bytes: Array[Byte], offset: Position, size: Size): Unit = synchronized {
    assume(usageCount >= 1)
    assume(offset.value <= Int.MaxValue)
    assume(size.value <= Int.MaxValue)
    randomAccessFile.seek(position + headerBytes)
    randomAccessFile.write(bytes, offset.value toInt, size.value toInt)
    updateDataPrint(bytes, offset.value.toInt, size.value.toInt)
  }
  
  final def eraseData(position: Position, size: Size): Unit = synchronized {
    assume(usageCount >= 1)
    assume(size.value <= Int.MaxValue)
    val bytes = new Array[Byte](size.value.toInt)
    randomAccessFile.seek(position.value + DataFile2.headerBytes)
    val read = fillFrom(randomAccessFile, bytes, 0, bytes.length)
    updateDataPrint(bytes, 0, read)
    randomAccessFile.seek(position.value + DataFile2.headerBytes)
    randomAccessFile.write(new Array[Byte](size.value.toInt))
    assume(read == size.value)
  }  
}

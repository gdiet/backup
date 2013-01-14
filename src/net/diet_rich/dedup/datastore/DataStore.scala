// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.datastore

import java.io._
import java.util.concurrent.Semaphore
import net.diet_rich.util.io._
import net.diet_rich.util.vals._

class DataStore(baseDir: File, dataSize: Size) { import DataStore._
  private def path(position: Position) = {
    val fileIndex = position.value / dataSize.value
    f"$fileIndex%010X".grouped(2).mkString("/")
  }

  private def file(dataPath: String) =
    baseDir.child(dirName).child(dataPath)

  private val dataFiles = collection.mutable.Map[String, (RandomAccessFile, Int)]()

  private def closeSurplusFiles: Unit = if (dataFiles.size > concurrentDataFiles)
    dataFiles.find { case (_, (_, 0)) => true; case _ => false }
    .foreach { case (dataPath, (dataFile, 0)) =>
      dataFile.close(); dataFiles.remove(dataPath)
    }
  
  @annotation.tailrec
  final def writeToStore(position: Position, bytes: Array[Byte], offset: Position, size: Size): Unit = {
    val dataPath = path(position)
    val dataFile = dataFiles.synchronized {
      val (dataFile, count) =
        dataFiles.get(dataPath).getOrElse {
          file(dataPath).getParentFile.mkdirs
          (new RandomAccessFile(file(dataPath), "rw"), 0)
        }
      assume (count >= 0, s"usage count for dataFile $dataPath is negative")
      dataFiles.put(dataPath, (dataFile, count+1))
      closeSurplusFiles
      dataFile
    }
    val dataOffset = position.value % dataSize.value
    val bytesToCopy = Size(math.min(dataSize.value - dataOffset, size.value))
    dataFile.synchronized{
      dataFile.seek(dataOffset + headerBytes)
      dataFile.write(bytes, offset.value toInt, bytesToCopy.value toInt)
    }
    dataFiles.synchronized {
      val (dataFile, count) = dataFiles(dataPath)
      dataFiles.put(dataPath, (dataFile, count-1))
    }
    if (bytesToCopy < size)
      writeToStore(position + bytesToCopy, bytes, offset + bytesToCopy, size - bytesToCopy)
  }
  
  def shutdown: Unit = dataFiles.synchronized {
    dataFiles.foreach { case (dataPath, (dataFile, count)) =>
      if (count != 0) throw new IllegalStateException(s"usage count for dataFile $dataPath is not zero but $count")
      dataFile.close
    }
    dataFiles.clear
  }
}

object DataStore {
  val dirName = "data"
  val concurrentDataFiles = 20
  val headerBytes = 16
}

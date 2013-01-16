// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.datastore

import java.io._
import net.diet_rich.util.io._
import net.diet_rich.util.vals._

class DataStore(baseDir: File, val dataSize: Size) { import DataStore._

  private val dataDir = baseDir.child(dirName)
  
  private def pathInDataDir(position: Position) =
    f"${position.value / dataSize.value}%010X".grouped(2).mkString("/")

  private def dataFile(pathInDir: String) = dataDir.child(pathInDir)

  private val dataFiles = collection.mutable.Map[String, DataFile]()
  private val fileQueue = collection.mutable.Queue[String]()

  private def newDataFile(position: Position, pathInDir: String) = {
    val file = new DataFile(position, dataFile(pathInDir))
    dataFiles.put(pathInDir, file)
    file
  }

  private def acquireDataFile(position: Position, pathInDir: String) = {
    val file = dataFiles.get(pathInDir).getOrElse(newDataFile(position, pathInDir))
    file.acquire
    file
  }
  
  private def closeSurplusFiles = {
    synchronized {
      if (fileQueue.size > concurrentDataFiles) {
        val path = fileQueue.dequeue
        Some((path, dataFiles(path)))
      } else None
    } match {
      case Some((path, file)) =>
        file.closeIfUnused
        synchronized { file.synchronized {
          if (file.isClosedAndUnused)  {
            dataFiles.remove(path)
          } else {
            fileQueue.enqueue(path)
          }
        } }
      case _ => Unit
    }
  }
  
  final def writeToSingleDataFile(position: Position, bytes: Array[Byte], offset: Position, size: Size): Unit = {
    assume((position.value % dataSize.value) + size.value <= dataSize.value, f"position: $position / data size: $dataSize / size: $size")
    val path = pathInDataDir(position)
    val file = synchronized(acquireDataFile(position, path))
    try {
      closeSurplusFiles
      val dataOffset = position.value % dataSize.value + headerBytes
      file.write(dataOffset, bytes, offset, size)
    } finally {
      file.release
    }
  }
  
  def shutdown: Unit = synchronized {
    dataFiles.values.foreach(_.close)
    dataFiles.clear
    fileQueue.clear
  }
}

object DataStore {
  val dirName = "data"
  val concurrentDataFiles = 30
  val headerBytes = 16
}

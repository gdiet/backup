// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.datastore

import java.io._
import net.diet_rich.util.io._
import net.diet_rich.util.vals._

class DataStore2[DFile <: DataFile2] private (baseDir: File, dataSize: Size, dFileFactory: (Position, File) => DFile) { import DataStore2._

  private val dataDir = baseDir.child(dirName)
  
  private def pathInDataDir(position: Position) =
    f"${position.value / dataSize.value}%010X".grouped(2).mkString("/")

  private def dataFile(pathInDir: String) = dataDir.child(pathInDir)

  private val dataFiles = collection.mutable.Map[String, DFile]()
  private val fileQueue = collection.mutable.Queue[String]()

  private def newDataFile(position: Position, pathInDir: String) = {
    val file = dFileFactory(position, dataFile(pathInDir))
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

  protected final def readWrite[Result](func: (DFile, Long, Array[Byte], Position, Size) => Result)
    (position: Position, bytes: Array[Byte], offset: Position, size: Size): Result = {
      assume((position.value % dataSize.value) + size.value <= dataSize.value, s"position: $position / data size: $dataSize / size: $size")
      val path = pathInDataDir(position)
      val file = synchronized(acquireDataFile(position, path))
      try {
        closeSurplusFiles
        val dataOffset = position.value % dataSize.value
        func(file, dataOffset, bytes, offset, size)
      } finally {
        file.release
      }
    }
  
  final def shutdown: Unit = synchronized {
    dataFiles.values.foreach(_.close)
    dataFiles.clear
    fileQueue.clear
  }
}

trait ReadOnlyDataStore extends DataStore2[DataFileRead] {
  final def readFromSingleDataFile =
    readWrite((file, dataOffset, bytes, offset, size) => file.read(dataOffset, bytes, offset, size)) _
}

trait WriteDataStore extends DataStore2[DataFileWrite] {
  final def writeToSingleDataFile =
    readWrite((file, dataOffset, bytes, offset, size) => file.writeNewData(dataOffset, bytes, offset, size)) _
}

object DataStore2 {
  val dirName = "data"
  val concurrentDataFiles = 30
  def readOnly(baseDir: File, dataSize: Size) =
    new DataStore2(baseDir, dataSize, (position, file) => new DataFile2(position, file) with DataFileRead) with ReadOnlyDataStore
  def writeOnly(baseDir: File, dataSize: Size) =
    new DataStore2(baseDir, dataSize, (position, file) => new DataFile2(position, file) with DataFileWrite) with WriteDataStore
}

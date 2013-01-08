// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.datastore

import java.io.File
import net.diet_rich.util.io._
import net.diet_rich.util.vals._

class DataStore(baseDir: File, dataSize: Size) { import DataStore._
  private def path(position: Position) = {
    val fileIndex = position.value / dataSize.value
    "%010X".format(fileIndex).grouped(2).mkString("/")
  }

  private val dataFiles = collection.mutable.Map[String, (DataFile, Int)]()

  private def storeSurplusDataFile: Unit = if (dataFiles.size > concurrentDataFiles) {
    val surplus = dataFiles.synchronized {
      val found = dataFiles.find { case (_, (_, count)) => count == 0 }
      found.foreach(_._2._1.dirty.set(false))
      found
    }
    surplus.foreach { case (dataPath, (dataFile, _)) =>
      dataFile.write
      dataFiles.synchronized{
        if ((!dataFile.dirty.get()) && dataFiles.get(dataPath).map(_._2 == 0).getOrElse(false))
          dataFiles.remove(dataPath)
      }
    }
  }
  
  private def withDataFile(position: Position)(code: DataFile => Long): Long = {
    val dataPath = path(position)
    val dataFile = dataFiles.synchronized {
      println("%s fetch  df: %s" format (Thread.currentThread(), dataPath.substring(9)))
      val (dataFile, count) = dataFiles.getOrElse(dataPath,
        ({val df = new DataFile(dataSize, baseDir.child(dataPath)); println("%s create df: %s" format (Thread.currentThread(), dataPath.substring(9))); df}, 0)
      )
      dataFiles.put(dataPath, (dataFile, count+1))
      dataFile
    }
    storeSurplusDataFile
    try {
      code(dataFile)
    } finally {
      dataFiles.synchronized {
        val (dataFile, count) = dataFiles(dataPath)
        dataFiles.put(dataPath, (dataFile, count-1))
      }
    }
  }

  @annotation.tailrec
  final def writeToStore(position: Position, bytes: Array[Byte], offset: Position, size: Size): Unit = {
    val written = Size(withDataFile(position){ dataFile =>
      val dataOffset = position.value % dataSize.value
      val bytesToCopy = math.min(dataSize.value - dataOffset, size.value)
      Array.copy(bytes, offset.value toInt, dataFile.bytes, dataOffset.toInt + DataFile.headerSize, bytesToCopy toInt)
      bytesToCopy
    })
    if (written < size) {
      writeToStore(position + written, bytes, offset + written, size - written)
    }
  }
  
  def shutdown: Unit = dataFiles.synchronized {
    dataFiles.values.foreach { case (dataFile, count) =>
      if (count != 0) throw new IllegalStateException("usage count for dataFile %s is not zero" format dataFile.file)
      dataFile.write
    }
    dataFiles.clear
  }
}

object DataStore {
  val dirName = "data"
  val concurrentDataFiles = 6
}

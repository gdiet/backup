// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.datastore

import java.io.File
import java.util.concurrent.{Callable, ExecutionException, Executors}
import net.diet_rich.util.io.EnhancedFile
import net.diet_rich.util.vals.Position

object DataStore2 {
  val dirName = "data"
  val concurrentDataFiles = 30
}

class DataStore2(baseDir: File, val dataSize: Int, readonly: Boolean) { import DataStore2._

  private val executor = Executors.newSingleThreadExecutor()
  private def execute[T](f: => T): T = try {
    executor.submit(new Callable[T] { override def call() = f }).get()
  } catch { case e: ExecutionException => throw e.getCause }

  private val dataDir: File = baseDir.child(dirName)
  private def pathInDataDir(dataFileNumber: Long) = f"$dataFileNumber%010X".grouped(2).mkString("/")
  private def dataFile(dataFileNumber: Long): File = dataDir.child(pathInDataDir(dataFileNumber))
  
  private val dataFileHandlers = collection.mutable.LinkedHashMap[Long, DataFile2]()

  def shutdown: Unit = {
    executor.shutdown
    executor.awaitTermination(Long.MaxValue, java.util.concurrent.TimeUnit.DAYS)
    dataFileHandlers.values.foreach(_.close)
  }

  private def acquireDataFile(dataFileNumber: Long) = {
    val dataFileHandler =
      dataFileHandlers.remove(dataFileNumber)
      .getOrElse(new DataFile2(dataFileNumber, dataFile(dataFileNumber), readonly))
    dataFileHandlers.put(dataFileNumber, dataFileHandler)
    if (dataFileHandlers.size > concurrentDataFiles)
      dataFileHandlers.remove(dataFileHandlers.keys.head).get.close
    dataFileHandler
  }

  private def dataFileNumberAndOffset(position: Position, size: Int) = {
    val dataFileNumber = position.value / dataSize
    val offsetInFileData = (position.value % dataSize).toInt
    assume(offsetInFileData + size <= dataSize, s"offsetInFileData: $offsetInFileData / dataSize: $dataSize / size: $size")
    (dataFileNumber, offsetInFileData)
  }
  
  def eraseDataInSingleDataFile(position: Position, size: Int): Unit = {
    val (dataFileNumber, offsetInFileData) = dataFileNumberAndOffset(position, size)
    execute {
      val dataFileHandler = acquireDataFile(dataFileNumber)
      dataFileHandler.eraseData(offsetInFileData, size)
    }
  }
  
  def overwriteDataInSingleDataFile(position: Position, bytes: Array[Byte], offsetInArray: Int, size: Int): Unit = {
    val (dataFileNumber, offsetInFileData) = dataFileNumberAndOffset(position, size)
    val dataPrint = DataFile2.calcDataPrint(offsetInFileData, bytes, offsetInArray, size)
    execute {
      val dataFileHandler = acquireDataFile(dataFileNumber)
      dataFileHandler.overwriteData(offsetInFileData, bytes, offsetInArray, size, dataPrint)
    }
  }
  
  def writeNewDataToSingleDataFile(position: Position, bytes: Array[Byte], offsetInArray: Int, size: Int): Unit = {
    val (dataFileNumber, offsetInFileData) = dataFileNumberAndOffset(position, size)
    val dataPrint = DataFile2.calcDataPrint(offsetInFileData, bytes, offsetInArray, size)
    execute {
      val dataFileHandler = acquireDataFile(dataFileNumber)
      dataFileHandler.writeNewData(offsetInFileData, bytes, offsetInArray, size, dataPrint)
    }
  }
  
  def readFromSingleDataFile(position: Position, bytes: Array[Byte], offsetInArray: Int, size: Int): Int = {
    val (dataFileNumber, offsetInFileData) = dataFileNumberAndOffset(position, size)
    execute {
      val dataFileHandler = acquireDataFile(dataFileNumber)
      dataFileHandler.read(offsetInFileData, bytes, offsetInArray, size)
    }
  }
}

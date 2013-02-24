// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.datastore

import java.io.File
import java.util.concurrent.{Callable, ExecutionException, Executors, ExecutorService}
import net.diet_rich.util.io._
import net.diet_rich.util.vals.Position

object DataStore2 {
  val dirName = "data"
  val concurrentDataFiles = 30
}

class DataStore2(baseDir: File, val dataSize: Int, readonly: Boolean) { import DataStore2._

  private val threads = 4
  private val executors = new Array[ExecutorService](threads)
  for (n <- 0 until threads) executors(n) = Executors.newSingleThreadExecutor()
  private def execute[T](dataFileNumber: Long)(f: => T): T = try {
    executors((dataFileNumber%threads).toInt).submit(new Callable[T] { override def call() = f }).get()
  } catch { case e: ExecutionException => throw e.getCause }

  private val dataDir: File = baseDir.child(dirName)
  private def pathInDataDir(dataFileNumber: Long) = f"$dataFileNumber%010X".grouped(2).mkString("/")
  private def dataFile(dataFileNumber: Long): File = dataDir.child(pathInDataDir(dataFileNumber))
  
  private val dataFileHandlerArray = new Array[collection.mutable.Map[Long, DataFile2]](threads)
  for (n <- 0 until threads) dataFileHandlerArray(n) = collection.mutable.LinkedHashMap[Long, DataFile2]()
  private def dataFileHandlers(dataFileNumber: Long) = dataFileHandlerArray((dataFileNumber%threads).toInt)

  def shutdown: Unit = {
    executors.foreach(_.shutdown)
    executors.foreach(_.awaitTermination(Long.MaxValue, java.util.concurrent.TimeUnit.DAYS))
    dataFileHandlerArray.foreach(_.values.foreach(_.close))
  }

  private def acquireDataFile(dataFileNumber: Long, mayCheckHeader: Boolean) = {
    val dfMap = dataFileHandlers(dataFileNumber)
    val dataFileHandler =
      dfMap.remove(dataFileNumber)
      .getOrElse(new DataFile2(dataFileNumber, dataFile(dataFileNumber), mayCheckHeader, readonly))
    dfMap.put(dataFileNumber, dataFileHandler)
    if (dfMap.size > concurrentDataFiles)
      dfMap.remove(dataFileHandlers(dataFileNumber).keys.head).get.close
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
    execute(dataFileNumber) {
      val dataFileHandler = acquireDataFile(dataFileNumber, true)
      dataFileHandler.eraseData(offsetInFileData, size)
    }
  }
  
  def overwriteDataInSingleDataFile(position: Position, bytes: Array[Byte], offsetInArray: Int, size: Int): Unit = {
    val (dataFileNumber, offsetInFileData) = dataFileNumberAndOffset(position, size)
    val dataPrint = DataFile2.calcDataPrint(offsetInFileData, bytes, offsetInArray, size)
    execute(dataFileNumber) {
      val dataFileHandler = acquireDataFile(dataFileNumber, true)
      dataFileHandler.overwriteData(offsetInFileData, bytes, offsetInArray, size, dataPrint)
    }
  }
  
  def writeNewDataToSingleDataFile(position: Position, bytes: Array[Byte], offsetInArray: Int, size: Int): Unit = {
    val (dataFileNumber, offsetInFileData) = dataFileNumberAndOffset(position, size)
    val dataPrint = DataFile2.calcDataPrint(offsetInFileData, bytes, offsetInArray, size)
    execute(dataFileNumber) {
      val dataFileHandler = acquireDataFile(dataFileNumber, true)
      dataFileHandler.writeNewData(offsetInFileData, bytes, offsetInArray, size, dataPrint)
    }
  }
  
  def readFromSingleDataFile(position: Position, bytes: Array[Byte], offsetInArray: Int, size: Int): Int = {
    val (dataFileNumber, offsetInFileData) = dataFileNumberAndOffset(position, size)
    execute(dataFileNumber) {
      val dataFileHandler = acquireDataFile(dataFileNumber, true)
      dataFileHandler.read(offsetInFileData, bytes, offsetInArray, size)
    }
  }
  
  def recreateDataFileHeader(dataFileNumber: Long): Boolean = execute(dataFileNumber) {
    val file = dataFile(dataFileNumber)
    if (file.exists()) {
      val dataFileHandler = acquireDataFile(dataFileNumber, false)
      dataFileHandler.recalculatePrint
      true
    } else false
  }
}

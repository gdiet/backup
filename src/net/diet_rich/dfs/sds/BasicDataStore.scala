// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dfs.sds

import java.io.File
import java.io.IOException
import net.diet_rich.util.ASSUME
import net.diet_rich.util.Bytes
import net.diet_rich.util.Configuration
import scala.collection.mutable.LinkedHashMap
import scala.collection.immutable.Stream
import scala.annotation.tailrec
import BasicDataStore._

class BasicDataStore(datadir: File, runtimeConfiguration: Configuration, configuration: Configuration) {

  ASSUME(datadir.isDirectory, "data directory <%s> is not a directory." format datadir)

  protected val dataLength = configuration.long("dataLength")
  protected val openFiles = runtimeConfiguration.long("openFiles")

  protected def fileOffset(offset: Long) = offset - offset % dataLength
  protected def fileName(offset: Long) = "%015d" format fileOffset(offset)
  protected def dataFileFor(offset: Long) = new File(datadir, fileName(offset)) // TODO introduce sensible files-per-dir (in 2 or more levels)

  protected val openDataFiles = LinkedHashMap[Long, CachedDataFile]()

  protected def dataFile(offset: Long) = {
    val entryOffset = fileOffset(offset)
    openDataFiles get entryOffset getOrElse {
      if (openDataFiles.size >= openFiles)
        openDataFiles.remove(openDataFiles.keys.head).get.flush
      val result = new CachedDataFile(entryOffset, dataLength, dataFileFor(offset))
      if (!result.isValidRead) throw new IOException("data file %s is corrupt." format dataFileFor(offset))
      openDataFiles += ((entryOffset, result))
      result
    }
  }
  
  def write(offset: Long, data: Bytes) : Unit =
    dataFile(offset) write(offset, data) foreach { remainder =>
      write(offset + data.size - remainder.size, remainder)
    }
  
  def read(offset: Long, len: Long) : Iterator[Bytes] =
    new Iterator[Bytes] {
      var position = offset
      var rest = len
      override def hasNext = rest > 0
      override def next : Bytes = {
        val read = dataFile(position).dataView(position, rest)
        position = position + read.size
        rest = rest - read.size
        read
      }
    }

  def flush : Unit = openDataFiles.values.foreach(_.flush)
  
}

object BasicDataStore {
  val storeConfigFile = "config.txt"
  val defaultConfig = Map("dataLength" -> "4000000")
  val defaultSystemConfig = Map("openFiles" -> "10")
    
  def apply(datadir: File, systemDefaults: Map[String, String] = defaultSystemConfig, storeDefaults: Map[String, String] = defaultConfig) : BasicDataStore = {
    val configuration = Configuration(new File(datadir, storeConfigFile), storeDefaults)
    new BasicDataStore(datadir, Configuration(systemDefaults), configuration)
  }
  
}

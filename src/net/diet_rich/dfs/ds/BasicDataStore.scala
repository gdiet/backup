package net.diet_rich.dfs.ds

import java.io.File
import net.diet_rich.util.ASSUME
import net.diet_rich.util.data.Bytes
import scala.collection.mutable.LinkedHashMap
import java.io.IOException

class BasicDataStore(datadir: File) extends DataStore {

  ASSUME(datadir.isDirectory, "data directory <" + datadir + "> is not a directory.")
  
  protected val dataLength = 1000 // TODO store in directory configuration file
  protected val openFiles = 10 // TODO make a backup system configuration property

  protected def fileOffset(offset: Long) = offset - offset % dataLength
  
  protected def fileName(offset: Long) = "%015d" format fileOffset(offset)
  
  // TODO introduce sensible files-per-dir (in 2 or more levels)
  protected def dataFileFor(offset: Long) = new File(datadir, fileName(offset))
  
  protected val openDataFiles = LinkedHashMap[Long, CachedDataFile]()

  protected def dataFile(offset: Long) = {
    val entryOffset = fileOffset(offset)
    openDataFiles get entryOffset getOrElse {
      if (openDataFiles.size >= openFiles) {
        val entryToRemove = openDataFiles.keys head
        val removed = (openDataFiles remove entryToRemove get)
        removed writeData
      }
      val result = new CachedDataFile(entryOffset, dataLength, dataFileFor(offset))
      if (!result.readData) throw new IOException("data file %s is corrupt" format dataFileFor(offset))
      openDataFiles += ((entryOffset, result))
      result
    }
  }
  
  def write(offset: Long, data: Bytes) = checkClosed {
    dataFile(offset) write(offset, data) foreach { remainder =>
      write(offset + data.length - remainder.length, remainder)
    }
  }
  
  def read(offset: Long, size: Long) : Bytes = checkClosed {
    dataFile(offset).read(offset,size)
    throw new AssertionError
  }
  
  protected var closed = false
  
  def close : Unit = {
    closed = true
    // TODO add close operations
  }

  protected def checkClosed[Result](task: => Result) = {
    if (closed) throw new IllegalStateException("Data store is already closed.")
    task
  }
  
}
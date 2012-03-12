package net.diet_rich.dfs.ds

import java.io.File
import net.diet_rich.util.ASSUME
import net.diet_rich.util.data.Bytes

class BasicDataStore(datadir: File) extends DataStore {

  ASSUME(datadir.isDirectory, "data directory <" + datadir + "> is not a directory.")
  
  protected def dataLength = 1000 // TODO store in directory config file
  // TODO introduce sensible files-per-dir (in 2 or more levels)
  
  def write(offset: Long, data: Bytes) = checkClosed { throw new AssertionError }
  
  def read(offset: Long, size: Long) : Bytes = checkClosed { throw new AssertionError }
  
  protected var closed = false
  
  def close() : Unit = {
    closed = true
    // TODO add close operations
  }

  protected def checkClosed[Result](task: => Result) = {
    if (closed) throw new IllegalStateException("Data store is already closed.")
    task
  }
  
}
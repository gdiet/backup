package net.diet_rich.dfs.ds

import net.diet_rich.util.data.Bytes

trait DataStore {

  def write(offset: Long, data: Bytes) : Unit
  def read(offset: Long, size: Long) : Bytes
  def close() : Unit
  
}
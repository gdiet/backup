package net.diet_rich.backup.util

trait MemoryManager {
  
  private val rt = Runtime.getRuntime
  
  def getLargeArray(size: Long): Option[Array[Byte]] = {
    require(size >= 0)
    val intSize = if (size > Int.MaxValue) Int.MaxValue else size.toInt
    val used = rt.totalMemory() - rt.freeMemory()
    val available = rt.maxMemory() - used
    if (available / 3 < size) None else Some(new Array[Byte](intSize))
  }

}
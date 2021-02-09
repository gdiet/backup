package dedup2.store

class LongTermStore {
  private val storeSize = 10000000
  private val store = new Array[Byte](storeSize)

  def write(position: Long, data: Array[Byte]): Unit = synchronized {
    require(position >= 0)
    val actualLength = math.max(math.min(data.length, storeSize - position), 0).toInt
    if (actualLength > 0) System.arraycopy(data, 0, store, position.toInt, actualLength)
  }

  def read(position: Long, size: Long): Array[Byte] = if (size == 0) Array() else synchronized {
    require(position >= 0)
    require(size >= 0)
    val actualLength = math.max(math.min(size, storeSize - position), 0).toInt
    val result = new Array[Byte](actualLength)
    if (actualLength > 0) System.arraycopy(store, position.toInt, result, 0, actualLength)
    result
  }
}

package dedup2

/** mutable! baseDataId can be -1. */
class DataEntry(val baseDataId: Long) {
  private var mem: Map[Long, Array[Byte]] = Map()
  private var _size: Long = 0
  def size: Long = synchronized(_size)
  def read(position: Long, size: Int): Array[Byte] = synchronized {
    new Array(size) // FIXME implement
  }
  def truncate(size: Long): Unit = synchronized { // FIXME test
    mem = mem.collect { case entry @ position -> data if position < size =>
      if (position + data.length > size) position -> data.take((size - position).toInt) else entry
    }
    _size = size
  }
  def write(offset: Long, dataToWrite: Array[Byte]): Unit = synchronized { // FIXME test
    val endOfWrite = offset + dataToWrite.length
    mem = mem.flatMap { case position -> data =>
      val prefix =
        if (position >= offset) None
        else Some(position -> data.take(math.min(offset - position, data.length).toInt))
      val postOff = position + data.length - endOfWrite
      val postfix =
        if (postOff <= 0) None
        else Some(position -> data.take(math.min(postOff, data.length).toInt))
      Seq(prefix, postfix).flatten
    } + (offset -> dataToWrite)
    _size = math.max(_size, endOfWrite)
  }
}

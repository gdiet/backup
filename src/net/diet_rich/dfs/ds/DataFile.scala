package net.diet_rich.dfs.ds

import java.io.File
import net.diet_rich.util.data.Bytes
import net.diet_rich.util.io.{using, RandomAccessFile, RandomAccessFileInput}
import net.diet_rich.util.data.Digester

class DataFile(val dataLength: Int, val file: File) {
  require (dataLength > 0)
  require (file != null)
  
  def readFullFile: Bytes = using(new RandomAccessFileInput(file)) { _ read dataLength extendMax }

  def readPart(off: Long, len: Long) : Bytes = {
    assert (off >= 0 && off < dataLength)
    using(new RandomAccessFileInput(file)) { input =>
      input seek (off)
      input read math.min(len, dataLength - off).toInt extendMax
    }
  }

  def writeAllData(bytes: Bytes) : Unit = {
    assert (bytes.length <= dataLength)
    using(new RandomAccessFile(file)) { _ write bytes }
  }

}
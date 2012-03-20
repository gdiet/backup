package net.diet_rich.dfs.sds

import java.io.File
import net.diet_rich.util.ASSUME
import net.diet_rich.util.io.using

class DataFile(val dataLength: Long, val file: File) {
  ASSUME (dataLength > 0, "data length of file %s must be positive but is %s" format (file, dataLength))
  ASSUME (dataLength <= Int.MaxValue, "data length of file %s must be an Integer value but is %s" format (file, dataLength))
  ASSUME (file != null, "data file must not be null")
  ASSUME (!file.exists || file.isFile, "data file %s must be a regular file if it exists" format file)
  
  def readAndPadZeros(off: Long = 0, len: Long = dataLength) : Bytes = {
    ASSUME (file.isFile, "data file %s must be an existing regular file" format file)
    IOUtil readFrom (file, off, Bytes(len)) _2
  }

  def writeFully(data: Bytes) : Unit = {
    ASSUME (data.size <= dataLength, "data size %s must be less or equal the data file length %s" format (data.size, dataLength))
    IOUtil write (file, data)
  }

}
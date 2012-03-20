package net.diet_rich.dfs.sds

import java.io.File
import java.io.RandomAccessFile
import net.diet_rich.util.io.using

object IOUtil {

  /** Blocks until requested length is read or end of stream is reached.
   */
  def readFrom(file: File, fileOffset: Long, data: Bytes) : (Long, Bytes) =
    using(new RandomAccessFile(file, "r")) { input =>
      input seek fileOffset
      @annotation.tailrec
      def readBytesTailRec(offset: Int) : Int =
        input.read(data.data, data.intOffset + offset, data.intSize - offset)
        match {
          case 0 => offset
          case read => readBytesTailRec(offset + read)
        }
      (readBytesTailRec(0), data)
    }

  def write(file: File, data: Bytes) : Unit =
    using(new RandomAccessFile(file, "rw")) { 
      _ write (data data, data intOffset, data intSize)
    }
  
}
package net.diet_rich.dedup.core

import java.io.{InputStream, RandomAccessFile, File}

import net.diet_rich.dedup.core.data.Bytes
import net.diet_rich.dedup.util._

trait Source {
  def read(count: Int): Bytes
  final def allData: Iterator[Bytes] = new Iterator[Bytes] {
    var currentBytes = read(0x8000)
    override def hasNext = currentBytes.length > 0
    override def next() = valueOf(currentBytes) before {currentBytes = read(0x8000)}
  }
}

trait SizedSource extends Source {
  def size: Long // FIXME only if known
}

trait FileLikeSource extends SizedSource with AutoCloseable {
  def reset(): Unit
}

object Source {
  private def readBytes(input: (Array[Byte], Int, Int) => Int, length: Int): Bytes = {
    val data = new Array[Byte](length)
    @annotation.tailrec
    def readRecurse(offset: Int, length: Int): Int =
      input(data, offset, length) match {
        case n if n < 1 => offset
        case n => if (n == length) offset + n else readRecurse(offset + n, length - n)
      }
    Bytes(data, 0, readRecurse(0, length))
  }

  def from(file: File): FileLikeSource = from(new RandomAccessFile(file, "r"))

  def from(file: RandomAccessFile): FileLikeSource = new FileLikeSource {
    override def size = file length()
    override def close() = file close()
    override def read(count: Int) = readBytes(file.read, count)
    override def reset(): Unit = file seek 0
  }

  def from(in: InputStream, expectedSize: Long): SizedSource = new SizedSource with AutoCloseable {
    override def size = expectedSize
    override def read(count: Int): Bytes = readBytes(in.read, count)
    override def close() : Unit = in close()
  }
}

// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import java.io.{InputStream, RandomAccessFile, File}

import net.diet_rich.dedup.core.data.Bytes
import net.diet_rich.dedup.util._

trait Source {
  def size: Long
  def read(count: Int): Bytes
  final def allData: Iterator[Bytes] = new Iterator[Bytes] {
    var currentBytes = read(0x8000)
    def hasNext = currentBytes.length > 0
    def next = valueOf(currentBytes) before {currentBytes = read(0x8000)}
  }
  def close(): Unit
}

trait ResettableSource extends Source {
  def reset: Unit
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

  def from(file: File): ResettableSource = from(new RandomAccessFile(file, "r"))

  def from(file: RandomAccessFile): ResettableSource = new ResettableSource {
    override def size = file length()
    override def close = file close()
    override def read(count: Int) = readBytes(file read (_,_,_), count)
    override def reset: Unit = file seek 0
  }

  def from(in: InputStream, expectedSize: Long): Source = new Source {
    override def size = expectedSize
    override def read(count: Int): Bytes = readBytes(in read (_,_,_), count)
    override def close() : Unit = in close()
  }
}

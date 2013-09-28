// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.backup

import java.io._
import net.diet_rich.dedup.vals._
import net.diet_rich.util.io._
import net.diet_rich.util.vals._

trait Source {
  def name: String
  def time: Time
  def size: Size
  def print[T](f: PrintedSource => T): T
}

case class SourceFile(file: File) extends Source {
  override def name: String = file getName
  override def time: Time = Time(file lastModified)
  override def size: Size = Size(file length)
  override def print[T](f: PrintedSource => T): T = {
    val in = new RandomAccessFile(file, "r")
    val header = in readBytes Print.size
    val print = Print(header)
    val source = if (header.length < Print.size) {
      in.close
      CompleteSource(this, print, Seq(header), Size(header length))
    } else {
      OpenSource(this, in, print, Seq(header))
    }
    f(source)
  }
}

sealed trait PrintedSource {
  protected def base: Source
  final def name: String = base name
  final def time: Time = base time
  def print: Print
  def size: Size
  def close: Unit
  def content: Iterator[Bytes]
  def cache(bytes: Iterator[Bytes]): PrintedSource
}

case class CompleteSource(base: Source, print: Print, content: Seq[Bytes], size: Size) extends PrintedSource {
  override def close = Unit
  override def cache(bytes: Iterator[Bytes]) = this
}

case class OpenSource(base: Source, in: RandomAccessFile, print: Print, dataStart: Seq[Bytes]) extends PrintedSource {
  override def size = Size(in length)
  override def close = in.close
  override def content = dataStart.iterator ++ dataTail
  override def cache(bytes: Iterator[Bytes]) = if (bytes isEmpty) this else {
    val cache = bytes.map(in.read(_)).takeWhile(!_.isEmpty).toSeq
    if (cache.last.isFull)
      copy(dataStart = dataStart ++ cache)
    else {
      in.close
      CompleteSource(base, print, dataStart ++ cache, cacheSize)
    }
  }
  def dataTail: Iterator[Bytes] = {
    in.seek(cacheSize.value)
    in.iterate
  }
  private val cacheSize = Size(dataStart map (_.length toLong) sum)
}

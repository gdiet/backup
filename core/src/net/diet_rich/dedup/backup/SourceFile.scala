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
  def name: String = file getName
  def time: Time = Time(file lastModified)
  def size: Size = Size(file length)
  def print[T](f: PrintedSource => T): T = {
    val in = new RandomAccessFile(file, "r")
    val header = in next Print.size
    val print = Print(header)
    val source = if (header.length < Print.size) {
      in.close
      CompleteSource(this, print, Seq(header))
    } else {
      OpenSource(this, in, print, Seq(header))
    }
    f(source)
  }
}

sealed trait PrintedSource {
  protected def base: Source
  def name: String = base.name
  def time: Time = base.time
  def print: Print
  def size: Size
  def close: Unit
  def content: BytesSource
  def notCached: Size
  def cache(bytes: Seq[Bytes]): PrintedSource
}

case class OpenSource(base: Source, in: JavaBytesSource, print: Print, dataStart: Seq[Bytes]) extends PrintedSource {
  override def size = base.size
  override def close = in.close
  override def content = linkedBytesSource(Seq(bytesSource(dataStart), in))
  override def notCached = size - Size(dataStart map (_.length toLong) sum)
  override def cache(bytes: Seq[Bytes]) = if (bytes isEmpty) this else {
    val cache = bytes.map(in.read(_))
    if (bytes.last.length > cache.last.length) {
      in.close
      CompleteSource(base, print, dataStart ++ cache)
    } else
      copy(dataStart = dataStart ++ cache)
  }
  in match {
    case e: SeekableJavaBytesSource =>
  }
}

case class CompleteSource(base: Source, print: Print, data: Seq[Bytes]) extends PrintedSource {
  override def size = Size(data map (_.length toLong) sum)
  override def close = Unit
  override def content = bytesSource(data)
  override def notCached = Size(0)
  override def cache(bytes: Seq[Bytes]) = this
}

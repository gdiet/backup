// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

import java.io._
import net.diet_rich.util.vals._
import scala.annotation.tailrec

package object io {
  type ByteSource = { def read(bytes: Array[Byte], offset: Position, length: Size): Size }
  type ByteSink = { def write(bytes: Array[Byte], offset: Position, length: Size): Unit }
  type Closeable = { def close(): Unit }
  type Seekable = { def seek(pos: Position): Unit }
  type Reader = ByteSource with Closeable
  type SeekReader = Seekable with Reader
  
  implicit class EnhancedInputStream(i: InputStream) {
    def asReader: Reader = new Object {
      def read(bytes: Array[Byte], offset: Position, length: Size): Size =
        Size(i.read(bytes, offset.intValue, length.intValue))
      def close(): Unit = i.close()
    }
  }

  implicit class EnhancedOutputStream(o: OutputStream) {
    def asByteSink: ByteSink = new Object {
      def write(bytes: Array[Byte], offset: Position, length: Size): Unit =
        o.write(bytes, offset.intValue, length.intValue)
    }
  }
  
  implicit class EnhancedRandomAccess(r: RandomAccessFile) {
    def asSeekReader: SeekReader = new Object {
      def read(bytes: Array[Byte], offset: Position, length: Size): Size =
        Size(r.read(bytes, offset.intValue, length.intValue))
      def close(): Unit = r.close()
      def seek(pos: Position): Unit = r.seek(pos.value)
    }
    def asByteSink: ByteSink = new Object {
      def write(bytes: Array[Byte], offset: Position, length: Size): Unit =
        r.write(bytes, offset.intValue, length.intValue)
    }
  }
  
  val emptyReader: SeekReader = new Object {
    def read(b: Array[Byte], off: Position, len: Size): Size = Size(0)
    def seek(pos: Position): Unit = Unit
    def close(): Unit = Unit
  }
  
  def using[Closeable <: io.Closeable, ReturnType] (resource: Closeable)(operation: Closeable => ReturnType): ReturnType =
    try { operation(resource) } finally { resource.close }

  def fillFrom(input: ByteSource, bytes: Array[Byte], offset: Position, length: Size): Size = {
    val maxPos = offset + length
    @tailrec
    def readRecurse(offset: Position): Position = {
      input.read(bytes, offset, maxPos - offset) match {
        case n if n < Size(1) => offset
        case n => if (offset + n == length) offset + n else readRecurse(offset + n)
      }
    }
    readRecurse(offset) - offset
  }

  def readAndDiscardAll(input: ByteSource): Size = {
    val buffer = new Array[Byte](8192)
    @tailrec
    def readRecurse(length: Size): Size =
      input.read(buffer, Position(0), Size(buffer.length)) match {
        case n if n < Size(1) => length
        case n => readRecurse(length + n)
      }
    readRecurse(Size(0))
  }

  def readSettingsFile(path: File): Map[String, String] =
    using(scala.io.Source.fromFile(path, "UTF-8")) { source =>
      source.getLines
      .map(_.trim)
      .filterNot(_.isEmpty)
      .filterNot(_.startsWith("#"))
      .map(_.split("[=:]", 2).map(_.trim()))
      .map{case Array(a,b) => (a,b)}
      .toMap
    }

  def writeSettingsFile(file: File, settings: Map[String, String]): Unit =
    using(new PrintWriter(file, "UTF-8")) { writer =>
      writer.write(
        settings
        .map{case (key, value) => s"$key = $value"}
        .mkString("\n")
      )
    }

  def sourceAsInputStream(source: ByteSource) = new java.io.InputStream {
    override def read: Int = {
      val array = new Array[Byte](1)
      read(array) match {
        case n if (n < 1) => -1
        case 1 => array(0)
        case n => throw new IOException(s"unexpected number of bytes read: $n")
      }
    }
    override def read(bytes: Array[Byte], offset: Int, length: Int): Int =
      source.read(bytes, Position(offset), Size(length)) match {
        case n if (n < Size(1)) => -1
        case n => n.intValue
      }
  }

  implicit class EnhancedFile(val value: File) extends AnyVal {
    def child(child: String): File = new File(value, child)
    def erase: Boolean = (
      if (value.isDirectory()) value.listFiles.forall(_.erase) else true
    ) && value.delete
  }
  
  // TODO make value class once nested class restriction is lifted
  implicit class EnhancedByteSource(val value: ByteSource) {
    def copyTo(sink: ByteSink): Size = {
      val buffer = new Array[Byte](32768)
      @annotation.tailrec
      def recurse(alreadRead: Size): Size = {
        value.read(buffer, Position(0), Size(buffer.length)) match {
          case n if (n > Size(0)) =>
            sink.write(buffer, Position(0), n)
            recurse(alreadRead + n)
          case _ => alreadRead
        }
      }
      recurse(Size(0))
    }
    
    def appendSource(source2: ByteSource): ByteSource = new Object {
      var switched = false
      var source = value
      def read(bytes: Array[Byte], internalOffset: Position, length: Size): Size =
        source.read(bytes, internalOffset, length) match {
          case n if (n < Size(1)) =>
            if (switched) n else {
              source = source2
              switched = true
              read(bytes, internalOffset, length)
            }
          case n => n
        }
    }
    
    def appendByte(byte: Byte): ByteSource =
      new EnhancedByteSource(value).appendSource(new java.io.ByteArrayInputStream(Array(byte)).asReader)
    
    def prependArray(data: Array[Byte], offset: Int, len: Int): ByteSource =
      new EnhancedByteSource(new java.io.ByteArrayInputStream(data, offset, len).asReader).appendSource(value)
  }
}

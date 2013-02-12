// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

import java.io._
import scala.annotation.tailrec

package object io {
  type ByteSource = { def read(bytes: Array[Byte], offset: Int, length: Int): Int }
  type ByteSink = { def write(bytes: Array[Byte], offset: Int, length: Int): Unit }
  type Closeable = { def close(): Unit } // FIXME use java.io.Closeable
  type Seekable = { def seek(pos: Long): Unit }
  type Reader = ByteSource with Closeable
  type SeekReader = Seekable with Reader

  val emptyReader: SeekReader = new Object {
    def read(b: Array[Byte], off: Int, len: Int): Int = 0
    def seek(pos: Long): Unit = Unit
    def close(): Unit = Unit
  }
  
  def using[Closeable <: io.Closeable, ReturnType] (resource: Closeable)(operation: Closeable => ReturnType): ReturnType =
    try { operation(resource) } finally { resource.close }

  def fillFrom(input: ByteSource, bytes: Array[Byte], offset: Int, length: Int): Int = {
    val maxPos = offset + length
    @tailrec
    def readRecurse(offset: Int): Int = {
      input.read(bytes, offset, maxPos - offset) match {
        case n if n < 1 => offset
        case n => if (offset + n == length) offset + n else readRecurse(offset + n)
      }
    }
    readRecurse(offset) - offset
  }

  def readAndDiscardAll(input: ByteSource) : Long = {
    val buffer = new Array[Byte](8192)
    @tailrec
    def readRecurse(length: Long): Long =
      input.read(buffer, 0, 8192) match {
        case n if n < 1 => length
        case n => readRecurse(length + n)
      }
    readRecurse(0)
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
      source.read(bytes, offset, length) match {
        case n if (n < 1) => -1
        case n => n
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
    def copyTo(sink: ByteSink) = {
      val bytes = new Array[Byte](32768)
      @annotation.tailrec
      def recurse(alreadRead: Long): Long = {
        value.read(bytes, 0, bytes.length) match {
          case n if (n > 0) =>
            sink.write(bytes, 0, n)
            recurse(alreadRead + n)
          case _ => alreadRead
        }
      }
      recurse(0)
    }
    
    def appendByte(byte: Byte): ByteSource = new Object {
      var isAppended = false
      def read(bytes: Array[Byte], internalOffset: Int, length: Int): Int = {
        value.read(bytes, internalOffset, length) match {
          case n if (n < 1) =>
            if (isAppended)
              n
            else {
              bytes(internalOffset) = byte
              isAppended = false
              1
            }
          case n => n
        }
      }
    }
    
    def prependArray(data: Array[Byte], offset: Int, len: Int): ByteSource = new Object {
      var read: Int = 0
      def read(bytes: Array[Byte], internalOffset: Int, length: Int): Int =
        if (read < len)
          if (length < len-read) {
            Array.copy(data, offset + read, bytes, internalOffset, length)
            read = read + length
            length
          } else {
            val result = len - read
            Array.copy(data, offset + read, bytes, internalOffset, result)
            read = len
            result
          }
        else
          value.read(bytes, internalOffset, length)
    }
  }
}

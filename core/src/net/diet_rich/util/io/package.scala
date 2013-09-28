// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

import net.diet_rich.util.vals.Bytes

package object io {
  def using[Closeable <: { def close: Unit }, ReturnType] (resource: Closeable)(operation: Closeable => ReturnType): ReturnType =
    try { operation(resource) } finally { resource.close }

  type JavaBytesSource = {
    def read(bytes: Array[Byte], offset: Int, length: Int): Int
  }
    
  def read(in: JavaBytesSource, numberOfBytes: Int): Bytes =
    read(in, Bytes(numberOfBytes))
    
  def read(in: JavaBytesSource, bytes: Bytes): Bytes = {
    @annotation.tailrec
    def readRecurse(offset: Int, bytesToRead: Int): Int = {
      in read(bytes.data, offset, bytesToRead) match {
        case n if n < 1 => offset
        case n if n == bytesToRead => offset + n
        case n => readRecurse(offset + n, bytesToRead - n)
      }
    }
    val read = readRecurse(bytes.offset, bytes.length) - bytes.offset
    bytes take read
  }

  def iterate(in: JavaBytesSource): Iterator[Bytes] =
    Iterator.continually(read(in, 32768)).takeWhile(_.length > 0)
  
  implicit class EnhancedJavaBytesSource(val in: JavaBytesSource) extends AnyVal {
    def read(bytes: Bytes): Bytes = net.diet_rich.util.io read(in, bytes)
    def readBytes(numberOfBytes: Int): Bytes = net.diet_rich.util.io read(in, numberOfBytes)
    def iterate: Iterator[Bytes] = net.diet_rich.util.io iterate(in)
  }
}

//  type JavaBytesSource = {
//    def read(bytes: Array[Byte], offset: Int, length: Int): Int
//    def close(): Unit
//  }
//
//  // FIXME can't match against types
//  type SeekableJavaBytesSource = JavaBytesSource {
//    def seek(position: Long): Unit
//  }
//  
//  trait BytesSource {
//    def next(numberOfBytes: Int): Bytes
//  }
//
//  trait StrictBytesSource extends BytesSource
//
//  def linkedBytesSource(sources: Seq[BytesSource]) = new BytesSource {
//    val iterator = sources.iterator
//    var currentSource: Option[BytesSource] = None
//    def next(numberOfBytes: Int): Bytes = currentSource match {
//      case None =>
//        if (iterator.hasNext) {
//          currentSource = Some(iterator.next)
//          next(numberOfBytes)
//        } else Bytes.empty
//      case Some(source) =>
//        source.next(numberOfBytes) match {
//          case bytes if bytes.length > 0 => bytes
//          case _ =>
//            currentSource = None
//            next(numberOfBytes)
//        }
//    }
//  }
//  
//  def bytesSource(bytes: Seq[Bytes]) = new BytesSource {
//    val iterator = bytes.iterator
//    var currentSlice: Option[Bytes] = None
//    def next(numberOfBytes: Int): Bytes = currentSlice match {
//      case None =>
//        if (iterator.hasNext) {
//          currentSlice = Some(iterator.next)
//          next(numberOfBytes)
//        } else Bytes.empty
//      case Some(bytes) =>
//        if (bytes.length <= numberOfBytes) {
//          currentSlice = None
//          bytes
//        } else {
//          currentSlice = Some(bytes.drop(numberOfBytes))
//          bytes.take(numberOfBytes)
//        }
//    }
//  }
//  
//  implicit class BytesSourceFromJava(val in: JavaBytesSource) extends StrictBytesSource {
//    def next(numberOfBytes: Int): Bytes = read(Bytes(numberOfBytes))
//    def read(bytes: Bytes): Bytes = {
//      @annotation.tailrec
//      def readRecurse(offset: Int, bytesToRead: Int): Int = {
//        in read(bytes.data, offset, bytesToRead) match {
//          case n if n < 1 => offset
//          case n if n == bytesToRead => offset + n
//          case n => readRecurse(offset + n, bytesToRead - n)
//        }
//      }
//      val read = readRecurse(bytes.offset, bytes.length) - bytes.offset
//      bytes take read
//    }
//  }
//}

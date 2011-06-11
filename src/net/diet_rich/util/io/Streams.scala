// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.io

object Streams {
  
  type InStream = { def read(buffer: Array[Byte], offset: Int, length: Int) : Int }
  type OutStream = { def write(buffer: Array[Byte], offset: Int, length: Int) : Unit }
  
  /** closes the resource after the operation */
  def using[Closeable <: {def close() : Unit}, ReturnType] (resource: Closeable)(operation: => ReturnType) : ReturnType =
    try { operation } finally { resource.close() }

  /** closes the resource after the operation */
  def usingIt[Closeable <: {def close() : Unit}, ReturnType] (resource: Closeable)(operation: (Closeable) => ReturnType) : ReturnType =
    try { operation(resource) } finally { resource.close() }

  /**
   * buffer factory used internally
   */
  private def createBuffer() = new Array[Byte](8192)
  
  /**
   * reads and discards a number of bytes from a stream. stops if end-of-stream is reached.
   * @return the number of bytes read.
   */
  def readSkip(source: InStream, length: Long = Long.MaxValue) : Long = {
    // Note: Designed tail recursively.
    val buffer = createBuffer()
    def readSome(alreadyRead: Long) : Long = {
      val maxread = math.min(length - alreadyRead, buffer.length) toInt
      val read = source.read(buffer, 0, maxread)
      if (read <= 0) {
        alreadyRead
      } else {
        readSome(alreadyRead + read)
      }
    }
    readSome(0)
  }

  /**
   * fills the array with bytes from a stream. stops if end-of-stream is reached, buffer is filled or limit is reached.
   * @return the number of bytes read.
   */
  def readBytes(source: InStream, buffer: Array[Byte], limit: Long = Long.MaxValue) : Int = {
    // Note: Designed tail recursively.
    val maxread = math.min(buffer.length, limit) toInt
    def readBytes(alreadyRead: Int) : Int = {
      val read = source.read(buffer, alreadyRead, maxread - alreadyRead)
      if (read <= 0) {
        alreadyRead
      } else {
        readBytes(alreadyRead + read)
      }
    }
    readBytes(0)
  }

  /**
   * Copy data from input to output until end-of-stream is reached.
   * @return the number of bytes copied.
   */
  def copyData(source: InStream, sink: OutStream) : Long = {
    // Note: Designed tail recursively.
    val copyBuffer = createBuffer()
    def copyDataRecursion(source: InStream, sink: OutStream, previouslyWritten: Long) : Long = {
      val read = source.read(copyBuffer, 0, copyBuffer.length)
      if (read <= 0) {
        assert(read < 0)
        previouslyWritten
      } else {
        sink.write(copyBuffer, 0, read)
        copyDataRecursion(source, sink, previouslyWritten + read)
      }
    }
    copyDataRecursion(source, sink, 0)
  }
 
}
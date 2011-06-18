// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.io

/**
 * utility methods for stream handling.
 * 
 * duck typing input stream and output stream allows to use normal streams
 * as well as RandomAccessFile objects and other stream like objects with
 * the utility methods.
 */
object Streams {
  import net.diet_rich.util.data.{Digest,Digester}
  
  /** closes the resource after the operation */
  def using[Closeable <: {def close() : Unit}, ReturnType] (resource: Closeable)(operation: => ReturnType) : ReturnType =
    try { operation } finally { resource.close }

  /** closes the resource after the operation */
  def usingIt[Closeable <: {def close() : Unit}, ReturnType] (resource: Closeable)(operation: (Closeable) => ReturnType) : ReturnType =
    try { operation(resource) } finally { resource.close }

  /** buffer factory used internally */
  private def createBuffer() = new Array[Byte](8192)
  
  /**
   * reads and discards data from a stream until limit or end-of-stream is reached.
   * 
   * @return the number of bytes read.
   */
  def readSkip(source: InputStream, limit: Long = Long.MaxValue) : Long = {
    copyData(source, OutputStream.empty, limit)
  }

  /**
   * copies data from input to output until limit or end-of-stream is reached.
   * 
   * @return the number of bytes copied.
   */
  def copyData(source: InputStream, sink: OutputStream, limit: Long = Long.MaxValue) : Long = {
    val copyBuffer = createBuffer()
    def copyDataRecursion(source: InputStream, sink: OutputStream, alreadyWritten: Long) : Long = {
      val maxread = math.min(copyBuffer.length, limit - alreadyWritten).toInt
      source.read(copyBuffer, 0, maxread) match {
        case bytesRead if bytesRead <= 0 => 
          assert(bytesRead == 0)
          alreadyWritten
        case bytesRead => 
          sink.write(copyBuffer, 0, bytesRead)
          copyDataRecursion(source, sink, alreadyWritten + bytesRead)
      }
    }
    copyDataRecursion(source, sink, 0)
  }

  def digestStream[DigestType](stream: InputStream, digester: Digester[DigestType]) : InputStream with Digest[DigestType] = {
    new InputStream with Digest[DigestType] {
      override def read(buffer: Array[Byte], offset: Int, length: Int) : Int = {
        val bytesRead = stream.read(buffer: Array[Byte], offset: Int, length: Int)
        digester.write(buffer, offset, bytesRead)
        bytesRead
      }
      def getDigest : DigestType = digester.getDigest
    }
  }
}
// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.io

trait InputStream {

  /**
   * if length is not zero, blocks until at least one byte is read 
   * or end of stream is reached.
   * 
   * @return the number of bytes read.
   */
  def read(buffer: Array[Byte], offset: Int, length: Int) : Int
  
  /**
   * blocks until requested length is read or end of stream is reached.
   * 
   * @return the number of bytes read.
   */
  def readFully(buffer: Array[Byte], offset: Int, length: Int) : Int = {
    require(offset >= 0)
    require(length >= 0)
    require(buffer.length >= offset + length)
    
    def readBytesRecursion(alreadyRead: Int) : Int = {
      read(buffer, offset + alreadyRead, length - alreadyRead) match {
        case bytesRead if bytesRead <= 0 => assert(bytesRead == 0); alreadyRead
        case bytesRead => readBytesRecursion(alreadyRead + bytesRead)
      }
    }
    readBytesRecursion(0)
  }

}
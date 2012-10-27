// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup

import java.io.RandomAccessFile
import java.security.MessageDigest
import net.diet_rich.util.io._

class HashCalcInput(input: RandomAccessFile, algorithm: String) { import HashCalcInput._
  private var digest = digester(algorithm)
  private var markDigester: Option[MessageDigest] = None
  private var markPosition: Long = 0
  def mark: Unit = {
    markPosition = input.getFilePointer
    markDigester = Some(digest.clone.asInstanceOf[java.security.MessageDigest])
  }
  def reset = {
    digest = markDigester.get
    markDigester = None
    input.seek(markPosition)
  }
  def remaining: Long = length - position
  def length: Long = input.length
  def position: Long = input.getFilePointer
  def read(bytes: Array[Byte], offset: Int, length: Int): Int = {
    val read = fillFrom(input, bytes, offset, length)
    digest.update(bytes, offset, read)
    read
  }
  def hash: Array[Byte] = digest.digest
  def close(): Unit = input.close()
}

object HashCalcInput {
  def markSupported(algorithm: String): Boolean = {
    try {
      digester(algorithm).clone
      true
    } catch {
    case e: CloneNotSupportedException => false
    }
  }
  
  def digester(algorithm: String) : MessageDigest =
    MessageDigest.getInstance(algorithm)
}

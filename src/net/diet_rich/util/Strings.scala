// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

object Strings {

  // FIXME check whether better to replace with fast Java implementation
  
  // originally taken from http://snippets.dzone.com/posts/show/8027
  def hex2Bytes(hex: String): Array[Byte] = {
    (for {i <- 0 to hex.length-1 by 2} yield hex.substring(i, i+2))
      .map(Integer.parseInt(_, 16).toByte).toArray
  }

  // originally taken from http://snippets.dzone.com/posts/show/8027
  def bytes2Hex(bytes: Array[Byte]): String = {
    def convert(byte: Byte): String = {
      (if ((byte & 0xff) < 0x10 ) "0" else "") + Integer.toHexString(byte & 0xff)
    }
    bytes.map(convert(_)).mkString.toUpperCase
  }

}
// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

object Strings {

  def bytes2Hex(bytes: Array[Byte]) : String = JStrings.bytes2Hex(bytes)
  
  def hex2Bytes(hex: String) : Array[Byte] = JStrings.hex2Bytes(hex)
  
}
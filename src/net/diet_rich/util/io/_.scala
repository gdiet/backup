// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

package object io {
  
  /** closes the resource after the operation */
  def using[Closeable <: {def close() : Unit}, ReturnType] (resource: Closeable)(operation: Closeable => ReturnType) : ReturnType =
    try { operation(resource) } finally { resource.close }

  type Reader = { def read(bytes: Array[Byte], offset: Int, length: Int): Int }
    
  def readFully(input: Reader, bytes: Array[Byte], offset: Int, length: Int) : Int = {
    def readRecurse(offset: Int): Int = {
      input.read(bytes, offset, length - offset) match {
        case -1 => offset
        case  n => readRecurse(offset)
      }
    }
    readRecurse(offset) - offset
  }
    
}

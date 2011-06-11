// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.datastore

trait DSWDataFileView {
  protected var index: Int
  protected val endIndex : Int
  protected def store(bytes: Array[Byte], offset: Int, length: Int, position: Int) : Unit
  def close : Unit

  /**
   * store as much data as fits in the data file view.
   * closes the view once filled. closed views always store 0 bytes.
   * thread safe synchronized.
   * @return the number of bytes stored
   */
  def store(bytes: Array[Byte], offset: Int, length: Int) : Int = synchronized {
    val numBytes = math.min(length, endIndex - index)
    if (numBytes > 0) {
      store(bytes, offset, numBytes, index)
      index = index + numBytes
      if (index == endIndex) close
    }
    numBytes
  }

}
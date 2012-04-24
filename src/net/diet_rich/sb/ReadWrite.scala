// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.sb

import java.io.File

trait ReadWrite {
  
  /** Store a file in the fileid node.
   * 
   *  @return data id, None if node was missing. */
  def write(fileid: Long, file: File) : Option[Long] = {
    throw new UnsupportedOperationException
  }
  
}
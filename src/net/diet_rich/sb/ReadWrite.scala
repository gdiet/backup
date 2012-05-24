// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.sb

import java.io.File

// this could become part of a high level API

//trait ReadWrite {
//  protected def tree: Tree
//  protected def datainfo: DataInfoDB
//  
//  /** Store a file in the fileid node.
//   * 
//   *  @return data id, None if node was missing. */
//  def write(fileid: Long, file: File) : Option[Long] = {
//    val dataid = datainfo.create
//    throw new UnsupportedOperationException
//  }
//  
//}
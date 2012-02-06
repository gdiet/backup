// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dfs

import DataDefinitions.FullFileData
import DataDefinitions.TimeAndData
import DataDefinitions.TimeSize
import net.diet_rich.util.io.InputStream

trait DFSFilesLogic extends DFSFiles {

  protected def cache: CacheForFiles

  final override def store(id: Long, input: FileDataAccess) : Boolean =
    throw new UnsupportedOperationException
  
  final override def store(id: Long, timeAndData: TimeAndData) : Boolean =
    cache store(id, timeAndData)

  final override def dataProperties(id: Long) : Option[FullFileData] =
    cache dataProperties id
    
  // EVENTUALLY consider just fetching time (less complex database access)
  final override def time(id: Long) : Option[Long] =
    dataProperties(id) map (_ time)
    
  final override def size(id: Long) : Option[Long] =
    dataProperties(id) map (_ size)

  final override def timeAndSize(id: Long) : Option[TimeSize] =
    dataProperties(id) map (info => TimeSize(info time, info size))
    
  final override def read(id: Long) : Option[InputStream] =
    throw new UnsupportedOperationException
    
}
// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.datastore

private[datastore]
class DSECFileWriter(settings: DSSettings, file: java.io.File, source: Option[java.io.File] = None)
extends net.diet_rich.util.logging.Logged {

  def write(data: Array[Byte], dataOffset: Int, dataLength: Int, position: Int) = {
    error("not implemented")
  }

}
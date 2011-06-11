// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.datastore

private[datastore]
class DSECFileWriter(
    protected val settings: DSSettings, 
    protected val file: java.io.File, 
    protected val source: Option[java.io.File] = None)
extends DSFileWriterTrait {

  /** update error correction data at the given position. */
  protected val storeMethod = (bytes: Array[Byte], offset: Int, length: Int, position: Int) =>
    for (n <- 0 until length) dataArray(position + n) = dataArray(position + n) ^ bytes(offset + n) toByte

}
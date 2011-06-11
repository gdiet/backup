// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.datastore

/**
 * always writes files of DSSettings.dataFileChunkSize + 8 bytes length (0-padded if necessary)
 * the last 8 bytes are the DSSettings.newDataFileChecksum Long checksum
 */
private[datastore]
class DSDataFileWriter(
    protected val settings: DSSettings, 
    protected val file: java.io.File, 
    protected val ecWriter: DSECFileWriter, 
    protected val source: Option[java.io.File] = None)
extends DSFileWriterTrait {

  /** store bytes at the given position, updating the error correction data accordingly. */
  protected val storeMethod = (bytes: Array[Byte], offset: Int, length: Int, position: Int) => {
    ecWriter.store(dataArray, position, length, position)
    for (n <- 0 until length) dataArray(position + n) = bytes(offset + n)
    ecWriter.store(dataArray, position, length, position)
  }

}

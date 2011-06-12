// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.datastore

private[datastore]
class DSWDataFile(
    protected val settings: DSSettings, 
    protected val fileID: Long, 
    protected val file: java.io.File, 
    protected val ecWriter: DSWECFile, 
    protected val source: Option[java.io.File] = None)
extends DSWFileTrait {

  private val dswDataFile = this

  /** protected for testability only. */
  protected val views = new collection.mutable.HashSet[DSWDataFileView]()

  /**
   * create a writer view for writing a part of the data file.
   * the DSWDataFile is closed as soon as the last DSWDataFileView is closed.
   * the management of views is thread safe synchronized.
   */
  def makeView(start: Int = 0, end: Int = settings.dataFileChunkSize) : DSWDataFileView =
    new DSWDataFileView { 
      dswDataFile.synchronized {
        require(isOpen)
        views += this
      }
      override protected var index = start
      override protected val endIndex = end
      override protected val fileID = dswDataFile.fileID
      override protected def store(bytes: Array[Byte], offset: Int, length: Int, position: Int) : Unit =
        dswDataFile.store(bytes, offset, length, position)
      override def close : Unit = {
        dswDataFile.synchronized {
          views -= this
          if (views isEmpty) dswDataFile.close
        }
      }
    }
  
  /** store bytes at the given position, updating the error correction data accordingly. */
  protected val storeMethod = (bytes: Array[Byte], offset: Int, length: Int, position: Int) => {
    ecWriter.store(dataArray, position, length, position)
    for (n <- 0 until length) dataArray(position + n) = bytes(offset + n)
    ecWriter.store(dataArray, position, length, position)
  }

}

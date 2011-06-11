// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.datastore

// TODO fetch settings from data store configuration file
private[datastore]
class DSSettings {

  /** maximum size of data chunk in data files. data file size is limited by Int.MaxValue. */
  val dataFileChunkSize : Int = 1000000
  
  /** factory for data file checksum objects */
  def newDataFileChecksum : java.util.zip.Checksum = new java.util.zip.Adler32
  
}
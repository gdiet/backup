package net.diet_rich.backup.datastore

// TODO fetch settings from data store configuration file
private[datastore]
class DSSettings {

  /** maximum size of data chunk in data files. */
  val dataFileChunkSize : Int = 1000000
  
  /** factory for data file checksum objects */
  def newDataFileChecksum : java.util.zip.Checksum = new java.util.zip.Adler32
  
}
// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.database

import org.specs2.SpecificationWithJUnit

import net.diet_rich.util.Hashes
import net.diet_rich.util.init

class DataInfoSpec extends SpecificationWithJUnit { def is = s2"""
  When trying to retrieve a data entry that does not exist, an exception should be thrown $retrieveMissingEntry
  """
  
  def zeroHash = Hash(Hashes zeroBytesHash "MD5") // FIXME 1 create Hash in Hashes
  def zeroPrint = CrcAdler8192.zeroBytesPrint

  implicit def connectionWithTable = init(TestDB.h2mem) { DataInfoDB.createTable(zeroHash, zeroPrint)(_) }
  lazy val dataInfo = new DataInfo()
  
  def retrieveMissingEntry = {
    (dataInfo dataEntry DataEntryID(1)) should throwA[NoSuchElementException]
  }
}

class DataInfo(implicit val connection: java.sql.Connection) extends DataInfoDB

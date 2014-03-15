// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.database

import org.specs2.SpecificationWithJUnit
import net.diet_rich.util.Hash
import net.diet_rich.util.init
import net.diet_rich.util.sql._

class DataInfoSpec extends SpecificationWithJUnit { def is = s2"""
  When trying to retrieve a data entry that does not exist, an exception should be thrown $retrieveMissingEntry
  """
  
  def zeroHash = Hash forEmptyData "MD5"
  def zeroPrint = CrcAdler8192.zeroBytesPrint

  implicit lazy val connectionWithTable = init(TestDB.h2mem) { DataInfoDB.createTable(zeroHash, zeroPrint)(_) }
  lazy val dataInfo = new ImplicitConnection() with DataInfoDB
  
  def retrieveMissingEntry = {
    (dataInfo dataEntry DataEntryID(1)) should throwA[NoSuchElementException]
  }
}

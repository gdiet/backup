// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.sql

import org.specs2.SpecificationWithJUnit

class SqlUtilSpec extends SpecificationWithJUnit { def is = s2"""
  A simple SQL update succeeds $sqlUpdate
  """

  implicit lazy val connection = TestDB.h2mem
  
  def sqlUpdate = update("DROP SEQUENCE noSuchSequence IF EXISTS;") === 0
}

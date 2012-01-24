// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dfs

import DataDefinitions.TimeAndData

trait SqlForFiles { self: SqlCommon =>

  private val storeTimeAndDataS = prepare(
      "UPDATE TreeEntries SET WHERE deleted = false AND parent = ?;")
  
  def store(id: Long, timeAndData: TimeAndData) : Unit =
    throw new UnsupportedOperationException
  
}
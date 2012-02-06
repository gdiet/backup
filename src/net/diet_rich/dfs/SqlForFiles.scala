// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dfs

import net.diet_rich.util.sql._
import DataDefinitions.FullFileData
import DataDefinitions.TimeAndData

trait SqlForFiles { self: SqlCommon =>

  private val storeTimeAndDataS = prepare(
      "UPDATE TreeEntries SET time = ?, dataid = ? WHERE id = ?;")
  private val allFileDataForIdS = prepare(
      "SELECT time, length, print, hash, dataid FROM TreeEntries JOIN DataInfo ON dataid = DataInfo.id WHERE TreeEntries.id = ?;")

  def store(id: Long, timeAndData: TimeAndData) : Boolean = {
    execUpdate(storeTimeAndDataS, timeAndData time, timeAndData dataId, id)
    true
  }
  
  def dataProperties(id: Long) : Option[FullFileData] =
    execQuery(allFileDataForIdS, id) {rs => FullFileData(rs long "time", rs long "length", rs long "print", rs bytes "hash", rs long "dataid")} headOnly

    
}
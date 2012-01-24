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
      "SELECT time, size, print, hash, dataid FROM TreeEntries JOIN DataInfo ON dataid = DataInfo.id WHERE id = ?;")

  def store(id: Long, timeAndData: TimeAndData) : Unit =
    execUpdate(storeTimeAndDataS, timeAndData time, timeAndData dataId, id)
  
  def dataProperties(id: Long) : Option[FullFileData] =
    execQuery(allFileDataForIdS, id) {rs => FullFileData(rs long "time", rs long "size", rs long "print", rs bytes "hash", rs long "dataid")} headOnly

    
}
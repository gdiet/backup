// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.sb

import TreeDB._
import df.IdAndName
import net.diet_rich.util.sql._
import scala.collection.immutable.Iterable
import net.diet_rich.util.Configuration._
import java.sql.Connection
import java.sql.SQLException
import scala.collection.mutable.SynchronizedQueue
import df.TimeDataId
import net.diet_rich.sb.df.TimeDataId
import net.diet_rich.sb.df.TimeDataId

trait TreeDataDB {
  def readOption(id: Long) : Option[TimeDataId]
  def read(id: Long) : TimeDataId = readOption(id) get
}

class TreeDataSqlDB(protected val connection: Connection) extends TreeDataDB with SqlDBCommon {
  protected val dataForId_ = prepare("SELECT time, dataid FROM TreeEntries WHERE id = ?;")

  override def readOption(id: Long) : Option[TimeDataId] =
    execQuery(dataForId_, id)(result => TimeDataId(result long 1, result long 2)) headOption
  
  // FIXME continue
  
}

object TreeDataSqlDB {
  def apply(connection: Connection) = new TreeSqlDB(connection)
}

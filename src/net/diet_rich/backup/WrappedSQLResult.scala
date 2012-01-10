// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup

import com.weiglewilczek.slf4s.Logging
import java.sql.ResultSet
import net.diet_rich.util.nullIsNone

class WrappedSQLResult(resultSet: ResultSet) extends Logging {
  private def log[S,T](column: S, value: T): T = {
    logger debug "result [" + column + "]: " + value
    value
  }
  def long(column: Int)           = log(column, resultSet getLong column)
  def long(column: String)        = log(column, resultSet getLong column)
  def longOption(column: Int)     = log(column, nullIsNone (resultSet getLong column) )
  def longOption(column: String)  = log(column, nullIsNone (resultSet getLong column) )
  def string(column: Int)         = log(column, resultSet getString column)
  def string(column: String)      = log(column, resultSet getString column)
}

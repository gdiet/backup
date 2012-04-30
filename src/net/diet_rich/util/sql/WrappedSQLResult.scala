// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.sql

import com.weiglewilczek.slf4s.Logging
import java.sql.ResultSet
import net.diet_rich.util.nullIsNone
import net.diet_rich.util.Bytes

class WrappedSQLResult(resultSet: ResultSet) extends Logging {
  private def log[S,T](column: S, value: T): T = {
    logger debug "result [" + column + "]: " + value
    value
  }
  private def asOption[T](value: T) : Option[T] = if (resultSet.wasNull) None else Some(value)
  def int(column: Int)         = log(column, resultSet getInt column)
  def intOption(column: Int)   = log(column, asOption (resultSet getInt column) )
  def long(column: Int)        = log(column, resultSet getLong column)
  def longOption(column: Int)  = log(column, asOption (log(column, resultSet getLong column)) )
  def string(column: Int)      = log(column, resultSet getString column)
  def stringOption(column: Int)= log(column, asOption (resultSet getString column) )
  def bytes(column: Int)       = log(column, resultSet getBytes column)
  def bytesOption(column: Int) = log(column, asOption (resultSet getBytes column) )
  
  def next: Boolean = resultSet.next
}

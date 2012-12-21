// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.util.sql

import java.sql.ResultSet

class WrappedSQLResult(resultSet: ResultSet) {
  private def asOption[T](value: T): Option[T] = if (resultSet.wasNull) None else Some(value)
  
  def int(column: Int): Int                         =           resultSet getInt    column
  def intOption(column: Int): Option[Int]           = asOption (resultSet getInt    column)
  def long(column: Int): Long                       =           resultSet getLong   column
  def longOption(column: Int): Option[Long]         = asOption (resultSet getLong   column)
  def string(column: Int): String                   =           resultSet getString column
  def stringOption(column: Int): Option[String]     = asOption (resultSet getString column)
  def bytes(column: Int): Array[Byte]               =           resultSet getBytes  column
  def bytesOption(column: Int): Option[Array[Byte]] = asOption (resultSet getBytes  column)
  
  def next: Boolean = resultSet.next
}

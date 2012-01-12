// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

import java.sql.PreparedStatement
import java.sql.Connection
import data.Bytes

package object sql {
  
  def setArguments(preparedStatement: ScalaThreadLocal[PreparedStatement], args: Any*): PreparedStatement =
    setArguments(preparedStatement(), args:_*)

  def setArguments(statement: PreparedStatement, args: Any*): PreparedStatement = {
    args.zipWithIndex foreach(_ match {
      case (x : Long, index)        => statement setLong (index+1, x)
      case (x : Int, index)         => statement setInt (index+1, x)
      case (x : String, index)      => statement setString (index+1, x)
      case (x : Boolean, index)     => statement setBoolean (index+1, x)
      case (x : Array[Byte], index) => statement setObject(index+1, x)
      case (x : Bytes, index)       => statement setObject(index+1, x toArray)
    })
    statement
  }
  
  def execQuery[T](stat: ScalaThreadLocal[PreparedStatement], args: Any*)(processor: WrappedSQLResult => T) : Stream[T] = {
    val resultSet = new WrappedSQLResult(setArguments(stat, args:_*) executeQuery)
    new Iterator[T] {
      var hasNextIsChecked = false
      var hasNextResult = false
      override def hasNext : Boolean = {
        if (!hasNextIsChecked) {
          hasNextResult = resultSet.next
          hasNextIsChecked = true
        }
        hasNextResult
      }
      override def next : T = {
        val hasNextResult = hasNext
        assert (hasNextResult)
        hasNextIsChecked = false
        processor(resultSet)
      }
    } toStream
  }
  
  def execUpdate(preparedStatement: ScalaThreadLocal[PreparedStatement], args: Any*) : Int =
    setArguments(preparedStatement, args:_*) executeUpdate()

  /** only use where performance is not a critical factor. */
  def execUpdateWithArgs(connection: Connection, command: String, args: Any*) : Int =
    setArguments(connection prepareStatement command, args:_*).executeUpdate()
  
}
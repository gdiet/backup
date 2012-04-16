// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

import java.sql.PreparedStatement
import java.sql.Connection

package object sql {
  
  def setArguments(statement: PreparedStatement, args: Any*): PreparedStatement = {
    args.zipWithIndex foreach(_ match {
      case (x : Long, index)        => statement setLong (index+1, x)
      case (x : Int, index)         => statement setInt (index+1, x)
      case (x : String, index)      => statement setString (index+1, x)
      case (x : Boolean, index)     => statement setBoolean (index+1, x)
      case (x : Array[Byte], index) => statement setObject(index+1, x)
      case (x : Bytes, index)       => statement setObject(index+1, x copyOfBytes)
    })
    statement
  }

  trait ResultIterator[T] extends Iterator[T] with HeadAndIterator[T] with NextOptIterator[T]

  def fetchOnlyLong(stat: PreparedStatement, args: Any*) : Long =
    execQuery(stat, args:_*)(_ long 1) headOnly

  def fetchOnlyString(stat: PreparedStatement, args: Any*) : String =
    execQuery(stat, args:_*)(_ string 1) headOnly

  /** only use where performance is not a critical factor. */
  def fetchOnlyString(connection: Connection, command: String, args: Any*) : String =
    fetchOnlyString(connection prepareStatement command, args:_*)
    
  def execQuery[T](stat: PreparedStatement, args: Any*)(processor: WrappedSQLResult => T) : ResultIterator[T] = {
    val resultSet = new WrappedSQLResult(setArguments(stat, args:_*) executeQuery)
    new ResultIterator[T] {
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
    }
  }
  
  /** only use where performance is not a critical factor. */
  def execQuery[T](connection: Connection, command: String, args: Any*)(processor: WrappedSQLResult => T) : ResultIterator[T] =
    execQuery(connection prepareStatement command, args:_*)(processor)
    
  def execUpdate(preparedStatement: PreparedStatement, args: Any*) : Int =
    setArguments(preparedStatement, args:_*) executeUpdate()

  /** only use where performance is not a critical factor. */
  def execUpdate(connection: Connection, command: String, args: Any*) : Int =
    setArguments(connection prepareStatement command, args:_*) executeUpdate()
  
}
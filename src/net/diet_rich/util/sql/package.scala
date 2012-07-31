// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

import java.sql.{Connection,PreparedStatement}

package object sql {

  trait SqlQuery {
    def apply[T](args: Any*)(processor: WrappedSQLResult => T): ResultIterator[T]
  }
  
  trait SqlUpdate {
    def apply(args: Any*): Int
  }
    
  trait ResultIterator[T] extends Iterator[T] {
    def nextOption: Option[T] =
      if (hasNext) Some(next) else None
    def nextOnly: T = {
      val result = next
      if (hasNext) throw new IllegalStateException("Expected no further results in result set.")
      result
    }
    def nextOptionOnly: Option[T] = {
      val result = nextOption
      if (hasNext) throw new IllegalStateException("Expected no further results in result set.")
      result
    }
  }
  
  def setArguments(statement: PreparedStatement, args: Any*): PreparedStatement = {
    args.zipWithIndex foreach(_ match {
      case (     x: Long,         index) => statement setLong   (index+1, x)
      case (Some(x: Long),        index) => statement setLong   (index+1, x)
      case (     x: Int,          index) => statement setInt    (index+1, x)
      case (Some(x: Int),         index) => statement setInt    (index+1, x)
      case (     x: String,       index) => statement setString (index+1, x)
      case (Some(x: String),      index) => statement setString (index+1, x)
      case (     x: Array[Byte],  index) => statement setObject (index+1, x)
      case (Some(x: Array[Byte]), index) => statement setObject (index+1, x)
      case (None, index) => statement setNull (index+1, statement.getParameterMetaData getParameterType (index+1))
      case (e, _) => throw new IllegalArgumentException("setArguments does not support %s type arguments" format e.getClass.getCanonicalName)
    })
    statement
  }

  def execQuery[T](stat: PreparedStatement, args: Any*)(processor: WrappedSQLResult => T): ResultIterator[T] = {
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
        if (!hasNext) throw new NoSuchElementException
        hasNextIsChecked = false
        processor(resultSet)
      }
    }
  }
  
  def execQuery[T](connection: Connection, command: String, args: Any*)(processor: WrappedSQLResult => T): ResultIterator[T] =
    execQuery(connection prepareStatement command, args:_*)(processor)

  def execUpdate(preparedStatement: PreparedStatement, args: Any*): Int =
    setArguments(preparedStatement, args:_*) executeUpdate()

  def execUpdate(connection: Connection, command: String, args: Any*): Int =
    setArguments(connection prepareStatement command, args:_*) executeUpdate()
  
  def prepareQuery(statement: String)(implicit connection: Connection): SqlQuery =
    new SqlQuery {
      protected val prepared =
        ScalaThreadLocal(connection prepareStatement statement, statement)
      override def apply[T](args: Any*)(processor: WrappedSQLResult => T): ResultIterator[T] =
        execQuery(prepared, args:_*)(processor)
    }

  def prepareUpdate(statement: String)(implicit connection: Connection): SqlUpdate =
    new SqlUpdate {
      protected val prepared =
        ScalaThreadLocal(connection prepareStatement statement, statement)
      override def apply(args: Any*): Int =
        execUpdate(prepared, args:_*)
    }
  
}
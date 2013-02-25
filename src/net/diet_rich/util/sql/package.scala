// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

import java.sql.{Connection, PreparedStatement}

package object sql {

  type WrappedConnection = {
    def con: java.sql.Connection
  }
  
  trait SqlQuery {
    def apply[T](args: Any*)(processor: WrappedSQLResult => T): ResultIterator[T]
  }
  
  trait SqlUpdate {
    def apply(args: Any*): Int
  }

  trait SingleRowSqlUpdate {
    def apply(args: Any*): Unit
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
  
  private def setArguments(statement: PreparedStatement, args: Any*): PreparedStatement = {
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
      case (e, _) => throw new IllegalArgumentException(s"setArguments does not support ${e.getClass.getCanonicalName} type arguments")
    })
    statement
  }

  private def execQuery[T](stat: PreparedStatement, args: Any*)(processor: WrappedSQLResult => T): ResultIterator[T] = {
    val resultSet = new WrappedSQLResult(setArguments(stat, args:_*).executeQuery)
    new ResultIterator[T] {
      var hasNextIsChecked = false
      var hasNextResult = false
      override def hasNext : Boolean = {
        if (!hasNextIsChecked) {
          hasNextResult = resultSet.next
          hasNextIsChecked = true
        }
        hasNextResult // TODO if no more elements, close the result set?
      }
      override def next : T = {
        if (!hasNext) throw new NoSuchElementException
        hasNextIsChecked = false
        processor(resultSet)
      }
    }
  }
  
  def execQuery[T](command: String, args: Any*)(processor: WrappedSQLResult => T)(implicit connection: Connection): ResultIterator[T] =
    execQuery(connection prepareStatement command, args:_*)(processor)

  private def execUpdate(preparedStatement: PreparedStatement, args: Any*): Int =
    setArguments(preparedStatement, args:_*) executeUpdate()

  private def execSingleRowUpdate(preparedStatement: PreparedStatement, args: Any*): Unit =
    setArguments(preparedStatement, args:_*).executeUpdate() match {
      case 1 => Unit
      case n => throw new IllegalStateException(s"SQL update $preparedStatement returned $n rows instead of 1")
    }
    
  def execUpdate(command: String, args: Any*)(implicit connection: Connection): Int =
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
  
  def prepareSingleRowUpdate(statement: String)(implicit connection: Connection): SingleRowSqlUpdate =
    new SingleRowSqlUpdate {
      protected val prepared =
        ScalaThreadLocal(connection prepareStatement statement, statement)
      override def apply(args: Any*): Unit =
        execSingleRowUpdate(prepared, args:_*)
    }
}

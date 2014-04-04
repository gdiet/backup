// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

import java.sql.{Connection, PreparedStatement}
import java.sql.Statement.RETURN_GENERATED_KEYS

package object sql {

  private class PreparedSql(statement: String)(implicit connection: Connection) {
    val prepared = ScalaThreadLocal(connection prepareStatement statement, statement)
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

  trait SqlInsertReturnKey {
    def apply(args: Any*): Long
  }
  
  trait ResultIterator[T] extends Iterator[T] {
    def resultSetName: String
    def nextOption: Option[T] =
      if (hasNext) Some(next) else None
    def nextOnly: T = {
      val result = next
      if (hasNext) throw new IllegalStateException(s"Expected no further results for $resultSetName.")
      result
    }
    def nextOptionOnly: Option[T] = {
      val result = nextOption
      if (hasNext) throw new IllegalStateException(s"Expected no further results for $resultSetName.")
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

  private def execQueryAka[T](stat: PreparedStatement, aka: => String, args: Any*)(processor: WrappedSQLResult => T): ResultIterator[T] = {
    val resultSet = new WrappedSQLResult(setArguments(stat, args:_*).executeQuery)
    new ResultIterator[T] {
      def resultSetName: String = aka format (args:_*)
      var hasNextIsChecked = false
      var hasNextResult = false
      override def hasNext : Boolean = {
        if (!hasNextIsChecked) {
          hasNextResult = resultSet.next
          hasNextIsChecked = true
        }
        hasNextResult // TODO 10 if no more elements, close the result set?
      }
      override def next : T = {
        if (!hasNext) throw new NoSuchElementException(s"Retrieving element from $resultSetName failed.")
        hasNextIsChecked = false
        processor(resultSet)
      }
    }
  }
  
  private def akaString(aka: String, statement: String, args: Seq[Any]) =
    if (aka isEmpty) s"'$statement' ${args.toList mkString ("(", ", ", ")")}" else aka
  
  def query[T](command: String, args: Any*)(processor: WrappedSQLResult => T)(implicit connection: Connection): ResultIterator[T] =
    execQueryAka(connection prepareStatement command, akaString("", command, args), args:_*)(processor)

  private def update(preparedStatement: PreparedStatement, args: Any*): Int =
    setArguments(preparedStatement, args:_*) executeUpdate()

  private def updateSingleRow(preparedStatement: PreparedStatement, args: Any*): Unit =
    setArguments(preparedStatement, args:_*).executeUpdate() match {
      case 1 => Unit
      case n => throw new IllegalStateException(s"SQL update $preparedStatement returned $n rows instead of 1")
    }
    
  def update(command: String, args: Any*)(implicit connection: Connection): Int =
    prepareUpdate(command) apply (args:_*)
  
// disabled due to h2 concurrency bug
//  def insertReturnKey(command: String, args: Any*)(implicit connection: Connection): Long =
//    prepareInsertReturnKey(command) apply (args:_*)
  
  def prepareQuery(statement: String, aka: String)(implicit connection: Connection): SqlQuery =
    new PreparedSql(statement) with SqlQuery {
      override def apply[T](args: Any*)(processor: WrappedSQLResult => T): ResultIterator[T] =
        execQueryAka(prepared, akaString(aka, statement, args), args:_*)(processor)
    }

  def prepareUpdate(statement: String)(implicit connection: Connection): SqlUpdate =
    new PreparedSql(statement) with SqlUpdate {
      override def apply(args: Any*): Int = update(prepared, args:_*)
    }
  
  def prepareSingleRowUpdate(statement: String)(implicit connection: Connection): SingleRowSqlUpdate =
    new PreparedSql(statement) with SingleRowSqlUpdate {
      override def apply(args: Any*): Unit = updateSingleRow(prepared, args:_*)
    }

  def prepareInsertReturnKey(statement: String)(implicit connection: Connection): SqlInsertReturnKey = {
    assert(false, "only use if each thread uses its own sql connection. Thread-local prepared statements are not enough.")
    new SqlInsertReturnKey {
      protected val prepared =
        ScalaThreadLocal(connection prepareStatement (statement, RETURN_GENERATED_KEYS), statement)
      override def apply(args: Any*): Long = {
        val statement = prepared.apply
        updateSingleRow(statement, args:_*)
        init(statement getGeneratedKeys) (_ next) getLong 1
      }
    }
  }

}

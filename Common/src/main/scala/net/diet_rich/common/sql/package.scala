package net.diet_rich.common

import java.sql.{ResultSet, Connection, PreparedStatement}
import java.sql.Statement.RETURN_GENERATED_KEYS

package object sql {
  def query(sql: String, aka: String = "")(implicit connection: Connection): SqlQuery =
    new PreparedSql(sql, aka) with SqlQuery {
      override def runv[T](args: Any*)(processor: ResultSet => T): ResultIterator[T] =
        execQueryAka(prepared(), args, akaString(sql, args, aka), processor)
    }

  def update(sql: String)(implicit connection: Connection): SqlUpdate =
    new PreparedSql(sql) with SqlUpdate {
      override def run(args: Seq[Any]): Int = setArguments(prepared(), args) executeUpdate()
    }

  def singleRowUpdate(sql: String)(implicit connection: Connection): SingleRowSqlUpdate =
    new PreparedSql(sql) with SingleRowSqlUpdate {
      override def run(args: Seq[Any]): Unit = updateSingleRow(prepared(), args, sql)
    }

  def insertReturnsKey(sql: String, indexOfKey: Int)(implicit connectionFactory: ScalaThreadLocal[Connection]): SqlInsertReturnKey = {
    new SqlInsertReturnKey {
      protected val prepared = ScalaThreadLocal(connectionFactory() prepareStatement (sql, Array(indexOfKey)))
      override def run(args: Seq[Any]): Long = {
        val statement = prepared()
        updateSingleRow(statement, args, sql)
        init(statement getGeneratedKeys()) (_ next()) long 1
      }
    }
  }

  sealed trait RunArgs[T] {
    def run(args: Seq[Any]): T
    final def runv(args: Any*): T = run(args)
    final def run(args: Product): T = run(args.productIterator.toSeq)
  }

  sealed trait SqlQuery { def runv[T](args: Any*)(processor: ResultSet => T): ResultIterator[T] } // FIXME runv and run
  sealed trait SqlUpdate extends RunArgs[Int] { def run(args: Seq[Any]): Int }
  sealed trait SingleRowSqlUpdate extends RunArgs[Unit] { def run(args: Seq[Any]): Unit }
  sealed trait SqlInsertReturnKey extends RunArgs[Long] { def run(args: Seq[Any]): Long }

  trait ResultIterator[T] extends Iterator[T] {
    protected def resultSetName: String
    protected def expectNoMoreResults = { _: Any =>
      if (hasNext) throw new IllegalStateException(s"Expected no further results for $resultSetName.")
    }
    def nextOption(): Option[T] = if (hasNext) Some(next()) else None
    def nextOnly(): T = init(next())(expectNoMoreResults)
    def nextOptionOnly(): Option[T] = init(nextOption())(expectNoMoreResults)
  }

  implicit class WrappedSQLResult(val resultSet: ResultSet) extends AnyVal {
    private def asOption[T](value: T): Option[T] = if (resultSet.wasNull) None else Some(value)
    def int(column: Int): Int                         =           resultSet getInt    column
    def intOption(column: Int): Option[Int]           = asOption (resultSet getInt    column)
    def long(column: Int): Long                       =           resultSet getLong   column
    def longOption(column: Int): Option[Long]         = asOption (resultSet getLong   column)
    def string(column: Int): String                   =           resultSet getString column
    def stringOption(column: Int): Option[String]     = asOption (resultSet getString column)
    def bytes(column: Int): Array[Byte]               =           resultSet getBytes  column
    def bytesOption(column: Int): Option[Array[Byte]] = asOption (resultSet getBytes  column)
  }

  private class PreparedSql(val sql: String, val aka: String = "")(implicit connection: Connection) {
    val prepared = ScalaThreadLocal(connection prepareStatement sql)
  }

  private def akaString(sql: String, args: Seq[Any], aka: String = "") =
    if (aka isEmpty()) s"'$sql' ${args.toList mkString ("(", ", ", ")")}" else aka

  private def execQueryAka[T](stat: PreparedStatement, args: Seq[Any], aka: => String, processor: ResultSet => T): ResultIterator[T] =
    new ResultIterator[T] {
      override def resultSetName: String = aka format (args:_*)
      val resultSet = setArguments(stat, args) executeQuery()
      var (hasNextIsChecked, hasNextResult) = (false, false)
      override def hasNext : Boolean = {
        if (!hasNextIsChecked) {
          hasNextResult = resultSet next()
          hasNextIsChecked = true
          if (!hasNextResult) resultSet close()
        }
        hasNextResult
      }
      override def next() : T = {
        if (!hasNext) throw new NoSuchElementException(s"Retrieving element from $resultSetName failed.")
        hasNextIsChecked = false
        processor(resultSet)
      }
    }

  private def setArguments(statement: PreparedStatement, args: Seq[Any]): PreparedStatement = {
    args.zipWithIndex foreach {
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
    }
    statement
  }

  private def updateSingleRow(preparedStatement: PreparedStatement, args: Seq[Any], sql: String): Unit =
    setArguments(preparedStatement, args).executeUpdate() match {
      case 1 => ()
      case n => throw new IllegalStateException(s"SQL update ${akaString(sql, args)} returned $n rows instead of 1")
    }

}

package net.diet_rich.common

import java.sql.{ResultSet, Connection, PreparedStatement}
import java.util.Date

package object sql {

  def query[T](sql: String)(implicit processor: ResultSet => T, connection: Connection): SqlQuery[ResultIterator[T]] =
    new PreparedSql(sql) with SqlQuery[ResultIterator[T]] {
      override def run(args: Seq[Any]): ResultIterator[T] =
        execQuery(prepared(), args, sql)
    }

  def update(sql: String)(implicit connection: Connection): SqlUpdate =
    new PreparedSql(sql) with SqlUpdate {
      override def run(args: Seq[Any]): Int = setArguments(prepared(), args) executeUpdate()
    }

  def singleRowUpdate(sql: String)(implicit connection: Connection): SingleRowSqlUpdate =
    new PreparedSql(sql) with SingleRowSqlUpdate {
      override def run(args: Seq[Any]): Unit = updateSingleRow(prepared(), args, sql)
    }

  def insertReturnsKey(sql: String, keyToReturn: String)(implicit connectionFactory: ScalaThreadLocal[Connection]): SqlInsertReturnKey = {
    new SqlInsertReturnKey {
      protected val prepared = ScalaThreadLocal(connectionFactory() prepareStatement (sql, Array(keyToReturn)))
      override def run(args: Seq[Any]): Long = {
        val statement = prepared()
        updateSingleRow(statement, args, sql)
        init(statement getGeneratedKeys()) (_ next()) long 1
      }
    }
  }

  sealed trait RunArgs[T] {
    def run(args: Seq[Any]): T
    final def run(): T = run(Seq())
    final def runv(args: Any*): T = run(args)
    final def run(args: Product): T = run(args.productIterator.toSeq)
  }

  sealed trait SqlQuery[T] extends RunArgs[T] { def run(args: Seq[Any]): T }
  sealed trait SqlUpdate extends RunArgs[Int] { def run(args: Seq[Any]): Int }
  sealed trait SingleRowSqlUpdate extends RunArgs[Unit] { def run(args: Seq[Any]): Unit }
  sealed trait SqlInsertReturnKey extends RunArgs[Long] { def run(args: Seq[Any]): Long }

  trait ResultIterator[T] extends Iterator[T] {
    protected def iteratorName: String
    protected def expectNoMoreResults = { _: Any =>
      if (hasNext) throw new IllegalStateException(s"Expected no further results for $iteratorName.")
    }
    def nextOption(): Option[T] = if (hasNext) Some(next()) else None
    def nextOnly(): T = init(next())(expectNoMoreResults)
    def nextOptionOnly(): Option[T] = init(nextOption())(expectNoMoreResults)
  }

  implicit class WrappedSQLResult(val resultSet: ResultSet) extends AnyVal {
    private def asOption[T](value: T): Option[T] = if (resultSet.wasNull) None else Some(value)
    def boolean(column: Int): Boolean                 =           resultSet getBoolean   column
    def bytes(column: Int): Array[Byte]               =           resultSet getBytes     column
    def bytesOption(column: Int): Option[Array[Byte]] = asOption (resultSet getBytes     column)
    def date(column: Int): Date                       =           resultSet getTimestamp column
    def int(column: Int): Int                         =           resultSet getInt       column
    def intOption(column: Int): Option[Int]           = asOption (resultSet getInt       column)
    def long(column: Int): Long                       =           resultSet getLong      column
    def longOption(column: Int): Option[Long]         = asOption (resultSet getLong      column)
    def string(column: Int): String                   =           resultSet getString    column
    def stringOption(column: Int): Option[String]     = asOption (resultSet getString    column)
  }

  private class PreparedSql(val sql: String)(implicit connection: Connection) {
    val prepared = ScalaThreadLocal(connection prepareStatement sql)
  }

  private def sqlWithArgsString(sql: String, args: Seq[Any]) = s"'$sql' ${args.toList mkString ("(", ", ", ")")}"

  private def execQuery[T](stat: PreparedStatement, args: Seq[Any], sql: String)(implicit processor: ResultSet => T): ResultIterator[T] =
    new ResultIterator[T] {
      def iteratorName = sqlWithArgsString(sql, args)
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
        if (!hasNext) throw new NoSuchElementException(s"Retrieving next element from $iteratorName failed.")
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
      case n => throw new IllegalStateException(s"SQL update ${sqlWithArgsString(sql, args)} returned $n rows instead of 1")
    }

}

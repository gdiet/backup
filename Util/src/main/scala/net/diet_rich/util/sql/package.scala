package net.diet_rich.util

import java.sql.{PreparedStatement, ResultSet}

package object sql {
  def transaction[T](f: => T)(implicit connectionFactory: ConnectionFactory): T = {
    val connection = connectionFactory()
    assert(connection.getAutoCommit, "Autocommit is already disabled.")
    try {
      connection.setAutoCommit(false)
      valueOf(f).before(connection.commit())
    } catch { case t: Throwable => connection.rollback(); throw t }
    finally connection.setAutoCommit(false)
  }

  def query[T](sql: String)(implicit processor: ResultSet => T, connectionFactory: ConnectionFactory): SqlQuery[ResultIterator[T]] =
    new PreparedSql(sql) with SqlQuery[ResultIterator[T]] {
      override def run(args: Seq[Any]): ResultIterator[T] =
        prepared() execQuery(args, sql)
    }

  def update(sql: String)(implicit connectionFactory: ConnectionFactory): SqlUpdate =
    new PreparedSql(sql) with SqlUpdate {
      override def run(args: Seq[Any]): Int = prepared() setArguments args executeUpdate()
    }

  def singleRowUpdate(sql: String)(implicit connectionFactory: ConnectionFactory): SingleRowSqlUpdate =
    new PreparedSql(sql) with SingleRowSqlUpdate {
      override def run(args: Seq[Any]): Unit = prepared().updateSingleRow(args, sql)
    }

  def insert(sql: String)(implicit connectionFactory: ConnectionFactory): SqlInsert = {
    new PreparedSql(sql) with SqlInsert {
      override def run(args: Seq[Any]): Unit = prepared().updateSingleRow(args, sql)
    }
  }

  def insertReturnsKey(sql: String, keyToReturn: String)(implicit connectionFactory: ConnectionFactory): SqlInsertReturnKey = {
    new SqlInsertReturnKey {
      private val prepared = ScalaThreadLocal.arm(() => connectionFactory().prepareStatement(sql, Array(keyToReturn)))
      override def run(args: Seq[Any]): Long = {
        val statement = prepared()
        statement updateSingleRow(args, sql)
        init(statement.getGeneratedKeys)(_.next()).long(1)
      }
      override def close(): Unit = prepared.close()
    }
  }

  sealed trait RunArgs[T] extends AutoCloseable {
    def run(args: Seq[Any]): T
    final def run(): T = run(Seq())
    final def run(arg: Any): T = run(Seq(arg))
    final def run(args: Product): T = run(args.productIterator.toSeq)
    final def runv(args: Any*): T = run(args)
  }

  sealed trait SqlQuery[T] extends RunArgs[T] { def run(args: Seq[Any]): T }
  sealed trait SqlUpdate extends RunArgs[Int] { def run(args: Seq[Any]): Int }
  sealed trait SingleRowSqlUpdate extends RunArgs[Unit] { def run(args: Seq[Any]): Unit }
  sealed trait SqlInsert extends RunArgs[Unit] { def run(args: Seq[Any]): Unit }
  sealed trait SqlInsertReturnKey extends RunArgs[Long] { def run(args: Seq[Any]): Long }

  implicit final class WrappedSQLResult(val resultSet: ResultSet) extends AnyVal {
    private def asOption[T](value: T): Option[T] = if (resultSet.wasNull) None else Some(value) // Note: 'value' must be used by-value
    def boolean(column: Int): Boolean                 =           resultSet getBoolean   column
    def bytes(column: Int): Array[Byte]               =           resultSet getBytes     column
    def bytesOption(column: Int): Option[Array[Byte]] = asOption (resultSet getBytes     column)
    def int(column: Int): Int                         =           resultSet getInt       column
    def intOption(column: Int): Option[Int]           = asOption (resultSet getInt       column)
    def long(column: Int): Long                       =           resultSet getLong      column
    def longOption(column: Int): Option[Long]         = asOption (resultSet getLong      column)
    def string(column: Int): String                   =           resultSet getString    column
    def stringOption(column: Int): Option[String]     = asOption (resultSet getString    column)
  }

  implicit private final class PreparedStatementMethods(val statement: PreparedStatement) extends AnyVal {
    def setArguments(args: Seq[Any]): PreparedStatement = {
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

    def updateSingleRow(args: Seq[Any], sql: String): Unit =
      setArguments(args).executeUpdate() match {
        case 1 => ()
        case n => throw new IllegalStateException(s"SQL update ${sqlWithArgsString(sql, args)} returned $n rows instead of 1")
      }

    def execQuery[T](args: Seq[Any], sql: String)(implicit processor: ResultSet => T): ResultIterator[T] =
      new ResultIterator[T] {
        def iteratorName: String = sqlWithArgsString(sql, args)
        private val resultSet = setArguments(args) executeQuery()
        var (hasNextIsChecked, hasNextResult) = (false, false)
        override def hasNext: Boolean = {
          if (!hasNextIsChecked) {
            hasNextResult = resultSet next()
            hasNextIsChecked = true
            if (!hasNextResult) resultSet close()
          }
          hasNextResult
        }
        override def next(): T = {
          if (!hasNext) throw new NoSuchElementException(s"Retrieving next element from $iteratorName failed.")
          hasNextIsChecked = false
          processor(resultSet)
        }
      }
  }

  private class PreparedSql(val sql: String)(implicit connectionFactory: ConnectionFactory) extends AutoCloseable {
    final val prepared: ArmThreadLocal[PreparedStatement] = ScalaThreadLocal.arm(() => connectionFactory() prepareStatement sql)
    final override def close(): Unit = prepared close()
  }

  private def sqlWithArgsString(sql: String, args: Seq[Any]) = s"'$sql' ${args.toList mkString ("(", ", ", ")")}"
}

package dedup.util

import java.sql.{PreparedStatement, ResultSet}

package object sql {
  implicit final class RichResultSet(val resultSet: ResultSet) extends AnyVal {
    private def asOption[T](value: T): Option[T] = if (resultSet.wasNull) None else Some(value)
    private def notNull[T](value: T): T = { assert(!resultSet.wasNull, s"null in $resultSet"); value }
    def boolean(column: Int): Boolean                 = notNull (resultSet getBoolean column)
    def bytes(column: Int): Array[Byte]               = notNull (resultSet getBytes   column)
    def bytesOption(column: Int): Option[Array[Byte]] = asOption(resultSet getBytes   column)
    def int(column: Int): Int                         = notNull (resultSet getInt     column)
    def intOption(column: Int): Option[Int]           = asOption(resultSet getInt     column)
    def long(column: Int): Long                       = notNull (resultSet getLong    column)
    def longOption(column: Int): Option[Long]         = asOption(resultSet getLong    column)
    def string(column: Int): String                   = notNull (resultSet getString  column)
    def stringOption(column: Int): Option[String]     = asOption(resultSet getString  column)
  }

  implicit final class RichPreparedStatement(val statement: PreparedStatement) extends AnyVal {
    def setArguments(args: Any*): PreparedStatement = {
      args.zipWithIndex foreach {
        case (     x: Array[Byte],  index) => statement setObject  (index+1, x)
        case (Some(x: Array[Byte]), index) => statement setObject  (index+1, x)
        case (     x: Boolean,      index) => statement setBoolean (index+1, x)
        case (Some(x: Boolean),     index) => statement setBoolean (index+1, x)
        case (     x: Int,          index) => statement setInt     (index+1, x)
        case (Some(x: Int),         index) => statement setInt     (index+1, x)
        case (     x: Long,         index) => statement setLong    (index+1, x)
        case (Some(x: Long),        index) => statement setLong    (index+1, x)
        case (     x: String,       index) => statement setString  (index+1, x)
        case (null, index) => statement setNull (index+1, statement.getParameterMetaData getParameterType (index+1))
        case (None, index) => statement setNull (index+1, statement.getParameterMetaData getParameterType (index+1))
        case (Some(x: String),      index) => statement setString (index+1, x)
        case (e, _) => throw new IllegalArgumentException(s"setArguments does not support ${e.getClass.getCanonicalName} type arguments")
      }
      statement
    }

    def query(args: Any*): ResultSet = setArguments(args).executeQuery()

    def updateSingleRow(args: Any*): Unit =
      setArguments(args).executeUpdate() match {
        case 1 => ()
        case n => throw new IllegalStateException(s"SQL update $statement $args returned $n rows instead of 1")
      }
  }
}

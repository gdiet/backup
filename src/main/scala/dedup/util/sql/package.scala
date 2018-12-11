package dedup.util

import java.sql.ResultSet

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
}

package dedup
package db

import java.io.File
import java.sql.{Connection, PreparedStatement, ResultSet, Statement}
import scala.util.Using.resource

def withConnection[T](dbDir: File, readonly: Boolean = true)(f: Connection => T): T =
  resource(H2.connection(dbDir, readonly))(f)
def withStatement(dbDir: File, readonly: Boolean = true)(f: Statement => Any): Unit =
  withConnection(dbDir, readonly)(_.withStatement(f))

extension (c: Connection)
  def withStatement[T](f: Statement => T): T = resource(c.createStatement())(f)
  /** Don't use nested or multi-threaded. */
  def transaction[T](f: => T): T =
    try { c.setAutoCommit(false); f.tap(_ => c.commit()) }
    catch { case t: Throwable => c.rollback(); throw t }
    finally c.setAutoCommit(true)

extension (stat: Statement)
  def query[T](queryString: String)(f: ResultSet => T): T = resource(stat.executeQuery(queryString))(f)

// Can't be opaque type Int because Int is already an SQL primitive
case class SqlNull(sqlType: Int)

type SqlPrimitive = Long|String|Array[Byte]|SqlNull

extension (stat: PreparedStatement)
  def query[T](f: ResultSet => T): T = resource(stat.executeQuery())(f)
  def set(params: (SqlPrimitive|DataId|Time)*): PreparedStatement =
    params
      .map(_.asInstanceOf[SqlPrimitive])
      .zipWithIndex.foreach {
        case (value: Int        , position: Int) => stat.setInt   (position + 1, value  )
        case (value: Long       , position: Int) => stat.setLong  (position + 1, value  )
        case (value: String     , position: Int) => stat.setString(position + 1, value  )
        case (value: Array[Byte], position: Int) => stat.setBytes (position + 1, value  )
        case (SqlNull(sqlType)  , position: Int) => stat.setNull  (position + 1, sqlType)
      }
    stat

extension (rs: ResultSet)
  /** Return None if the ResultSet.wasNull, else Some(f(resultSet)). */
  def opt[T](f: ResultSet => T): Option[T] = f(rs).pipe(t => if rs.wasNull then None else Some(t))
  def seq[T](f: ResultSet => T): Vector[T] = Iterator.continually(Option.when(rs.next)(f(rs))).takeWhile(_.isDefined).flatten.toVector

/** Ensure the ResultSet has a next element and apply it to f. */
def next[T](f: ResultSet => T): ResultSet => T = { rs => ensure("query.next", rs.next(), "Next element not available"); f(rs) }
/** Ensure the ResultSet has a SINGLE next element and apply it to f. */
def one[T](f: ResultSet => T): ResultSet => T = { rs => next(f)(rs).tap(_ => ensure("query.one", !rs.next(), "Unexpectedly another element is available")) }
/** Return None if the ResultSet is empty, else Some(f(resultSet)). */
def maybe[T](f: ResultSet => T): ResultSet => Option[T] = rs => Option.when(rs.next())(f(rs))
/** Return the single Long result of the ResultSet. */
def oneLong: ResultSet => Long = one(_.getLong(1))

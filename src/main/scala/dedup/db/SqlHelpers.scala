package dedup
package db

import java.io.File
import java.sql.{Connection, PreparedStatement, ResultSet, Statement}
import scala.util.Using.resource

def withConnection(dbDir: File, readonly: Boolean = true)(f: Connection => Any): Unit =
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

extension (stat: PreparedStatement)
  def query[T](f: ResultSet => T): T = resource(stat.executeQuery())(f)

extension (rs: ResultSet)
  def withNext[T](f: ResultSet => T): T = { ensure("query.next", rs.next(), "Next element not available"); f(rs) }
  def maybeNext[T](f: ResultSet => T): Option[T] = Option.when(rs.next())(f(rs))
  def one[T](f: ResultSet => T): T = withNext(f).tap(_ => ensure("query.one", !rs.next(), "Unexpectedly another element is available"))
  def opt[T](f: ResultSet => T): Option[T] = f(rs).pipe(t => if rs.wasNull then None else Some(t))
  def seq[T](f: ResultSet => T): Vector[T] = Iterator.continually(Option.when(rs.next)(f(rs))).takeWhile(_.isDefined).flatten.toVector

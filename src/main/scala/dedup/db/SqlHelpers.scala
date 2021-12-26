package dedup
package db

import java.sql.{Connection, ResultSet}

extension (c: Connection)
/** Don't use nested or multi-threaded. */
  def transaction[T](f: => T): T =
    try { c.setAutoCommit(false); f.tap(_ => c.commit()) }
    catch { case t: Throwable => c.rollback(); throw t }
    finally c.setAutoCommit(true)

extension (rs: ResultSet)
  def withNext[T](f: ResultSet => T): T = { require(rs.next()); f(rs) }
  def maybeNext[T](f: ResultSet => T): Option[T] = Option.when(rs.next())(f(rs))
  def one[T](f: ResultSet => T): T = withNext(f).tap(_ => require(!rs.next()))
  def opt[T](f: ResultSet => T): Option[T] = f(rs).pipe(t => if rs.wasNull then None else Some(t))
  def seq[T](f: ResultSet => T): Vector[T] = Iterator.continually(Option.when(rs.next)(f(rs))).takeWhile(_.isDefined).flatten.toVector

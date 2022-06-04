package dedup
package db

import java.sql.{Connection, DriverManager}
import scala.util.Using.resource

object MemH2:
  Class.forName("org.h2.Driver")
  // For SQL debugging, add to the DB URL "...;TRACE_LEVEL_SYSTEM_OUT=2"
//  def apply(f: Connection => Any): Unit = resource(DriverManager.getConnection("jdbc:h2:mem:;TRACE_LEVEL_SYSTEM_OUT=2"))(f)
  def apply(f: Connection => Any): Unit = resource(DriverManager.getConnection("jdbc:h2:mem:"))(f)

package net.diet_rich.fdfs

import java.sql.Connection
import java.sql.PreparedStatement
import java.util.concurrent.atomic.AtomicLong
import net.diet_rich.util.ScalaThreadLocal
import net.diet_rich.util.sql._

// FIXME check for things not needed anymore

protected trait SqlDBCommon {
  protected def readAsAtomicLong(statement: String)(implicit connection: Connection): AtomicLong = new AtomicLong(
    execQuery(connection, statement)(_ long 1) headOnly
  )

  protected def throwIllegalUpdateException(where: String, count: Int, id: Long) =
    throw new IllegalStateException("%s: Unexpected %s times update for id %s" format(where, count, id))
}

protected trait SqlDBObjectCommon {
  protected def tableName: String
  protected def debuggingConstraints: List[String] = List()
  
  final def addDebuggingConstraints(connection: Connection) : Unit =
    debuggingConstraints foreach(constraint => execUpdate(connection, "ALTER TABLE %s ADD CONSTRAINT %s;" format(tableName, constraint)))

  final def removeDebuggingConstraints(connection: Connection, constraints: List[String]) : Unit =
    debuggingConstraints foreach(constraint => execUpdate(connection,
      "ALTER TABLE %s DROP CONSTRAINT %s;" format(tableName, constraint split " " head)))
  
  def dropTable(connection: Connection) : Unit =
    execUpdate(connection, "DROP TABLE %s IF EXISTS;" format tableName)
}

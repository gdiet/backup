package net.diet_rich.fdfs

import java.sql.Connection
import java.util.concurrent.atomic.AtomicLong
import net.diet_rich.util.sql._

protected trait SqlDBCommon {
  protected def readAsAtomicLong(statement: String)(implicit connection: Connection): AtomicLong =
    new AtomicLong(execQuery(connection, statement)(_ long 1) headOnly)
}

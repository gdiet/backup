// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.db

import java.sql.Connection
import java.util.concurrent.atomic.AtomicLong
import net.diet_rich.util.sql._

object SqlDBUtil {
  def readAsAtomicLong(statement: String)(implicit connection: Connection): AtomicLong =
    new AtomicLong(execQuery(connection, statement)(_ long 1).nextOnly)
}

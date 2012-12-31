// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.database

import java.util.concurrent.atomic.AtomicLong
import net.diet_rich.util.sql._

object SqlDBUtil {
  def readAsAtomicLong(statement: String)(implicit connection: WrappedConnection): AtomicLong =
    new AtomicLong(execQuery(connection.con, statement)(_ long 1).nextOnly)
}

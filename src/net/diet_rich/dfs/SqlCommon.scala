// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dfs

import java.sql.Connection
import java.sql.PreparedStatement
import net.diet_rich.util.ScalaThreadLocal

trait SqlCommon {

  protected def connection: Connection 
  
  protected def prepare(statement: String) : ScalaThreadLocal[PreparedStatement] =
    ScalaThreadLocal(connection prepareStatement statement, statement)

}
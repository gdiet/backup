// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util

import java.sql.PreparedStatement

package object sql {
  
  def setArguments(preparedStatement: ScalaThreadLocal[PreparedStatement], args: Any*): PreparedStatement =
    setArguments(preparedStatement(), args:_*)

  def setArguments(statement: PreparedStatement, args: Any*): PreparedStatement = {
    args.zipWithIndex foreach(_ match {
      case (x : Long, index)    => statement setLong (index+1, x)
      case (x : Int, index)     => statement setInt (index+1, x)
      case (x : String, index)  => statement setString (index+1, x)
      case (x : Boolean, index) => statement setBoolean (index+1, x)
      case (x : Array[Byte], index) => statement setObject(index+1, x)
    })
    statement
  }
  
}
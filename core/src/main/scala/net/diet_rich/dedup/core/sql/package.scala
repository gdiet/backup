// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

package object sql {
  type CurrentDatabase = scala.slick.driver.JdbcDriver#Backend#Database
  type CurrentSession = scala.slick.driver.JdbcDriver#Backend#Session

  val databaseVersionKey = "database version"
  val databaseVersionValue = "2.0"
}

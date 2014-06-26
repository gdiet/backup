// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

package object sql {
  type Database = scala.slick.driver.JdbcDriver#Backend#Database
  type Session = scala.slick.driver.JdbcDriver#Backend#Session
}

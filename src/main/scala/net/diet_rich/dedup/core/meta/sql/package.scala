package net.diet_rich.dedup.core.meta

package object sql {
  type CurrentDatabase = scala.slick.driver.JdbcDriver#Backend#Database
  type CurrentSession = scala.slick.driver.JdbcDriver#Backend#Session
}

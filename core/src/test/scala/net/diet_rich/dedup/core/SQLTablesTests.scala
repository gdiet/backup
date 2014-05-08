// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import org.specs2.SpecificationWithJUnit
import scala.slick.driver.H2Driver.simple._
import net.diet_rich.dedup.core.values.TreeEntryID
import net.diet_rich.dedup.util.ThreadSpecific

class SQLTablesTests extends SpecificationWithJUnit { def is = s2"""
    FIXME $timingForWriteToTable
  """

  def timingForWriteToTable = {
    val database = Database forURL (
      url = "jdbc:h2:target/testdb;DB_CLOSE_ON_EXIT=FALSE",
      user = "sa",
      password = "",
      driver = "org.h2.Driver"
      )

    database.withSession { implicit session => SQLTables.createTables(16) }
    val tables = new SQLTables {
      val sessions = ThreadSpecific[Session] { database createSession }
    }
    val time = System.currentTimeMillis()
    for (i <- 1 to 100000) {
      tables.createTreeEntry(TreeEntryID(i), "name")
    }
    println(System.currentTimeMillis() - time)

    import scala.slick.jdbc.StaticQuery.interpolation
    database.withSession { implicit session => sqlu"shutdown compact" execute }

    failure
  }

//  def timingForReadFromTable = {
//    val database = Database forURL(
//      url = "jdbc:h2:target/testdb",
//      user = "sa",
//      password = "",
//      driver = "org.h2.Driver"
//      )
//
//    database withSession {
//      implicit session =>
//        SQLTables.createTreeTable
//        val tables = new SQLTables {
//          val dbSession = session
//
//          def treeQueryFilter: String = ""
//        }
//        val time = System.currentTimeMillis()
//        for (i <- 1 to 1000000) {
//          tables.treeEntry(TreeEntryID(i))
//        }
//        println(System.currentTimeMillis() - time)
//        failure
//    }
//  }
}

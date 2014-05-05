// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.database

import org.specs2.SpecificationWithJUnit
import scala.slick.driver.H2Driver.simple._
import net.diet_rich.dedup.values.{TreeEntryType, TreeEntryID}

class SQLTablesTests extends SpecificationWithJUnit { def is = s2"""
    A thing to test timingForReadFromTable
    A thing to test $timingForWriteToTable
  """

  def timingForWriteToTable = {
    val database = Database forURL (
      url = "jdbc:h2:target/testdb",
      user = "sa",
      password = "",
      driver = "org.h2.Driver"
    )

    database withSession { implicit session =>
      SQLTables.createTreeTable
      val tables = new SQLTables {
        val dbSession = session
        def treeQueryFilter: String = ""
      }
      val time = System.currentTimeMillis()
      for (i <- 1 to 100000) {
        tables.create(TreeEntryID(i), "name", TreeEntryType.DIR)
      }
      println(System.currentTimeMillis() - time)

      import scala.slick.jdbc.StaticQuery.interpolation
      sqlu"shutdown compact"

      failure
    }
  }

  def timingForReadFromTable = {
    val database = Database forURL(
      url = "jdbc:h2:target/testdb",
      user = "sa",
      password = "",
      driver = "org.h2.Driver"
      )

    database withSession {
      implicit session =>
        SQLTables.createTreeTable
        val tables = new SQLTables {
          val dbSession = session

          def treeQueryFilter: String = ""
        }
        val time = System.currentTimeMillis()
        for (i <- 1 to 1000000) {
          tables.treeEntry(TreeEntryID(i))
        }
        println(System.currentTimeMillis() - time)
        failure
    }
  }
}

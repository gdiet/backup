// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.database

import java.sql._
import net.diet_rich.util.logging.Logged

class Database extends Logged {

  Class.forName("org.hsqldb.jdbc.JDBCDriver")
  val connection = DriverManager.getConnection("jdbc:hsqldb:mem:mymemdb", "SA", "")
  val statement = connection.createStatement
  
  val prepGetRootEntry = connection.prepareStatement("SELECT id, parent, name, type FROM Entries WHERE id = 0;")
  
  def createTables = {
    val createTables = SQL.sectionsWithConstraints("create tables")
    createTables.foreach(_.split("\n").foreach(info(_)))
    statement.execute(createTables.mkString("\n"))
    
    val initializeTables = SQL.sectionsWithConstraints("initialize tables")
    initializeTables.foreach(_.split("\n").foreach(info(_)))
    statement.execute(initializeTables.mkString("\n"))
  }

  // EVENTUALLY use status flag in database to signal "connected / shut down orderly"

  val rootEntry = {
    val results   = prepGetRootEntry.executeQuery
    val hasResult = results.next
    assert (hasResult)
    val id = results.getLong(1)
    val parent = results.getLong(2)
    val name = results.getString(3)
    val typ = results.getString(4)
  }

  
  def entryForPath(path: String) = {
    require(path != "/")
    require(path == "" || path.startsWith("/"))
    statement.executeQuery("SELECT id, parent, name, type FROM Entries WHERE")
  }
  
  
}
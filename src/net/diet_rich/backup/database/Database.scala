// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.database

import java.sql._
import net.diet_rich.util.logging.Logged
import net.diet_rich.util.{Bytes,ScalaThreadLocal}

class Database extends Logged {

  Class.forName("org.hsqldb.jdbc.JDBCDriver")
  val connection = DriverManager.getConnection("jdbc:hsqldb:mem:mymemdb", "SA", "")
  val statement = connection.createStatement // use only synchronized!
  
  def prepareStatement(statement: String) = ScalaThreadLocal(connection.prepareStatement(statement))
  
  def createTables = synchronized {
    val createTables = SQL.sectionsWithConstraints("create tables").mkString("\n").split(";")
    createTables.foreach(command => {
      command.split("\n").foreach(info(_))
      statement.execute(command)
    })
    
    val initializeTables = SQL.sectionsWithConstraints("initialize tables").mkString("\n").split(";")
    initializeTables.foreach(command => {
      command.split("\n").foreach(info(_))
      statement.execute(command)
    })
  }

  // FIXME implement a possibility to skip DB creation
  createTables
  
  val getRootEntry  = prepareStatement("SELECT id, parent, name, type FROM Entries WHERE id = 0;")
  val getEntry      = prepareStatement("SELECT id, parent, name, type FROM Entries WHERE name = ? AND parent = ?;")
  
  // EVENTUALLY use status flag in database to signal "connected / shut down orderly"

  def query(preparedQuery: ScalaThreadLocal[PreparedStatement], args: Any*) = {
    val query = preparedQuery()
    args.zipWithIndex.foreach(_ match {
      case (x : Long, index)    => query.setLong(index, x)
      case (x : Integer, index) => query.setInt(index, x)
      case (x : String, index)  => query.setString(index, x)
      case (x : Boolean, index) => query.setBoolean(index, x)
      case (x : Bytes, index)   => query.setBytes(index, x.bytes)
    })
  }
  
  val rootEntry = {
    val results   = getRootEntry().executeQuery
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
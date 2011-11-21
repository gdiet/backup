// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.database

import java.sql._
import net.diet_rich.util.logging.Logged
import net.diet_rich.util.{Bytes,Choice,ScalaThreadLocal}

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
  
  val getEntry      = prepareStatement("SELECT id, parent, name, type FROM Entries WHERE name = ? AND parent = ?;")
  
  // EVENTUALLY use status flag in database to signal "connected / shut down orderly"

  def prepareStatement(preparedStatement: ScalaThreadLocal[PreparedStatement], args: Any*) = {
    val statement = preparedStatement()
    args.zipWithIndex.foreach(_ match {
      case (x : Long, index)    => statement.setLong(index, x)
      case (x : Int, index)     => statement.setInt(index, x)
      case (x : String, index)  => statement.setString(index, x)
      case (x : Boolean, index) => statement.setBoolean(index, x)
      case (x : Bytes, index)   => statement.setBytes(index, x.bytes)
    })
    statement
  }

  def executeUpdate(preparedStatement: ScalaThreadLocal[PreparedStatement], args: Any*) : Int = {
    prepareStatement(preparedStatement, args:_*).executeUpdate()
  }

  def executeQuery(preparedStatement: ScalaThreadLocal[PreparedStatement], args: Any*) = {
    val resultSet = prepareStatement(preparedStatement, args:_*).executeQuery()
    if (resultSet.next()) Some(new WrappedResult(resultSet)) else None
  }

  /* this is never the ROOT entry since root's parent is NULL */
  case class Entry(id: Long, parent: Long, name: String, typ: String)
  
  class WrappedResult(resultSet: ResultSet) {
    def next =
      if (resultSet.next()) new WrappedResult(resultSet) else None
    def long(column: Int) = resultSet.getLong(column)
    def long(column: String) = resultSet.getLong(column)
    def longOption(column: Int) = Choice.nullIsNone(resultSet.getLong(column))
    def longOption(column: String) = Choice.nullIsNone(resultSet.getLong(column))
    def string(column: Int) = resultSet.getString(column)
    def string(column: String) = resultSet.getString(column)
  }
  
  def entryForName(name: String, parent: Long) = {
    require(name != "")
    require(!name.contains("/"))
    executeQuery(getEntry, name, parent).map(results =>
      Entry(results.long("id"), results.long("parent"), results.string("name"), results.string("type"))
    )
  }
    
  def entryForPath(path: String, parent: Long = 0) = {
    require(path.startsWith("/"))
    require(!path.endsWith("/"))
    // split, then fold left
  }
  
}

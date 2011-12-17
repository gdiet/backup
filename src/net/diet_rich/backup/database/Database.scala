// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.database

import java.sql._
import net.diet_rich.util.logging.Logged
import net.diet_rich.util.{Bytes,Choice,ScalaThreadLocal}

class Database extends Logged {

  Class.forName("org.hsqldb.jdbc.JDBCDriver")
  val connection = DriverManager.getConnection("jdbc:hsqldb:mem:mymemdb", "SA", "")
  connection.setAutoCommit(true)
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

  // EVENTUALLY implement a possibility to skip DB creation
  createTables
  
  def setArguments(preparedStatement: ScalaThreadLocal[PreparedStatement], args: Any*) = {
    val statement = preparedStatement()
    args.zipWithIndex.foreach(_ match {
      case (x : Long, index)    => statement.setLong(index+1, x)
      case (x : Int, index)     => statement.setInt(index+1, x)
      case (x : String, index)  => statement.setString(index+1, x)
      case (x : Boolean, index) => statement.setBoolean(index+1, x)
      case (x : Bytes, index)   => statement.setBytes(index+1, x.bytes)
    })
    statement
  }

  def executeUpdate(preparedStatement: ScalaThreadLocal[PreparedStatement], args: Any*) : Int = {
    setArguments(preparedStatement, args:_*).executeUpdate()
  }

  def executeQuery(preparedStatement: ScalaThreadLocal[PreparedStatement], args: Any*) = {
    val resultSet = setArguments(preparedStatement, args:_*).executeQuery()
    if (resultSet.next()) Some(new WrappedResult(resultSet)) else None
  }

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
  
  val getEntryPS    = prepareStatement("SELECT id, parent, name, type FROM Entries WHERE name = ? AND parent = ?;")
  val addEntryPS    = prepareStatement("INSERT INTO Entries (id, parent, name, type) VALUES ( ? , ? , ? , ? );")
  
  // EVENTUALLY insert "status" flag in database to lock and signal "connected"

  def addEntry(id: Long, parent: Long, name: String, typ: String) = {
    executeUpdate(addEntryPS, id, parent, name, typ)
  }
  
  /** will never return the root (since the root's parent is NULL). */
  def entryForName(name: String, parent: Long) = {
    require(name != "")
    require(!name.contains("/"))
    executeQuery(getEntryPS, name, parent).map(results =>
      Entry(results.long("id"), results.long("parent"), results.string("name"), results.string("type"))
    )
  }
  
//  /** will never return the root (since the root's parent is NULL). */
//  def entryForPath(path: String, parent: Long = 0) = {
//    require(path.startsWith("/"))
//    require(!path.endsWith("/"))
//    path.split("/").tail.foldLeft[Option[Entry]](Some(new Entry(parent)))((parent, name) =>
//      parent.flatMap( parentEntry => entryForName(name, parentEntry.parent) )
//    )
//  }
  
  addEntry(1,0,"abc","DIR")

//  println("--" + entryForPath("/home/test"))
//  println("--" + entryForPath("/abc"))

}

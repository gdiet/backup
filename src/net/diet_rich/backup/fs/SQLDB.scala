// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.fs

import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import net.diet_rich.util.Choice
import net.diet_rich.util.ScalaThreadLocal

class SQLDB {
  // set up connection
  Class forName "org.hsqldb.jdbc.JDBCDriver"
  val connection = DriverManager getConnection("jdbc:hsqldb:mem:mymemdb", "SA", "")
  connection setAutoCommit true

  // create tables
  def createTables = synchronized {
    val statement = connection.createStatement
    SQL sectionsWithConstraints "create tables" mkString "\n" split ";" foreach statement.execute
    SQL sectionsWithConstraints "initialize tables" mkString "\n" split ";" foreach statement.execute
  }
  
  // FIXME implement a possibility to skip DB creation
  createTables

  // JDBC helper methods
  def prepareStatement(statement: String) = ScalaThreadLocal(connection prepareStatement statement)
  
  def setArguments(preparedStatement: ScalaThreadLocal[PreparedStatement], args: Any*) = {
    val statement = preparedStatement()
    args.zipWithIndex.foreach(_ match {
      case (x : Long, index)    => statement.setLong(index+1, x)
      case (x : Int, index)     => statement.setInt(index+1, x)
      case (x : String, index)  => statement.setString(index+1, x)
      case (x : Boolean, index) => statement.setBoolean(index+1, x)
    })
    statement
  }

  def executeUpdate(preparedStatement: ScalaThreadLocal[PreparedStatement], args: Any*) : Int = {
    setArguments(preparedStatement, args:_*).executeUpdate()
  }

  def executeQuery(preparedStatement: ScalaThreadLocal[PreparedStatement], args: Any*) : Option[WrappedResult] = {
    val resultSet = setArguments(preparedStatement, args:_*).executeQuery()
    if (resultSet.next()) Some(new WrappedResult(resultSet)) else None
  }

  def executeQueryIter(preparedStatement: ScalaThreadLocal[PreparedStatement], args: Any*) : Iterator[WrappedResult]{def nextOption: Option[WrappedResult]} = {
    val resultSet = setArguments(preparedStatement, args:_*).executeQuery()
    new Iterator[WrappedResult] {
      def hasNext = resultSet.next
      val next = new WrappedResult(resultSet)
      def nextOption = if (hasNext) Some(next) else None
    }
  }

  class WrappedResult(resultSet: ResultSet) {
    def long(column: Int) = resultSet.getLong(column)
    def long(column: String) = resultSet.getLong(column)
    def longOption(column: Int) = Choice.nullIsNone(resultSet.getLong(column))
    def longOption(column: String) = Choice.nullIsNone(resultSet.getLong(column))
    def string(column: Int) = resultSet.getString(column)
    def string(column: String) = resultSet.getString(column)
  }

  val getEntryForIdPS = 
    prepareStatement("SELECT id, parent, name, type FROM Entries WHERE deleted = false AND id = ?;")
  val getChildrenForIdPS = 
    prepareStatement("SELECT id FROM Entries WHERE deleted = false AND parent = ?;")
  val getEntryForNameAndParentPS = 
    prepareStatement("SELECT id, parent, name, type FROM Entries WHERE deleted = false AND name = ? AND parent = ?;")
  val addEntryIdParentNameTypePS = 
    prepareStatement("INSERT INTO Entries (id, parent, name, type) VALUES ( ? , ? , ? , ? );")

  /** Get the entry for an ID if any. */
  def get(id: Long) : Option[DBEntry] = synchronized {
    executeQueryIter(getEntryForIdPS, id)
    .nextOption
    .map (rs =>
      rs string "type" match {
        case "DIR" =>
          val children = executeQueryIter(getChildrenForIdPS, id).map(_.long("id"))
          DBDir(id, rs string "name", rs long "parent", children.toList)
        case "FILE" => throw new UnsupportedOperationException // FIXME
        case _ => throw new IllegalArgumentException
      }
    )
  }

}
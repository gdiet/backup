// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.sb

import java.sql.Connection
import net.diet_rich.util.StringMap
import net.diet_rich.util.sql._

object RepositoryInfoDB {

  val HASHALGORITHMKEY = "hash algorithm"
  val SHUTDOWNKEY = "shut down"
  val DBVERSIONKEY = "database version"
  
  def createTables(connection: Connection): Unit =
    execUpdate(connection, """
      CREATE CACHED TABLE RepositoryInfo (
        key VARCHAR(32) PRIMARY KEY,
        value VARCHAR(256) NOT NULL
      );
    """)

  def dropTables(connection: Connection): Unit =
    execUpdate(connection, "DROP TABLE RepositoryInfo IF EXISTS;")
    
  def readSettings(connection: Connection): StringMap =
    execQuery(connection, "SELECT key, value FROM RepositoryInfo;")(result => (result string 1, result string 2)) toMap

  def readSetting(connection: Connection, key: String): Option[String] =
    execQuery(connection, "SELECT value FROM RepositoryInfo WHERE key = ?;", key)(result => result string 1).nextOptionOnly
    
  def addOrUpdate(connection: Connection, key: String, value: String) : Unit = {
    delete(connection, key)
    execUpdate(connection, "INSERT INTO RepositoryInfo (key, value) VALUES (?, ?);", key, value)
  }

  def delete(connection: Connection, key: String) : Unit =
    execUpdate(connection, "DELETE FROM RepositoryInfo WHERE key = ?;", key)
  
}

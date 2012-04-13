package net.diet_rich.sb

import net.diet_rich.util.Configuration._
import net.diet_rich.util.sql._
import java.sql.Connection

object RepositoryInfoDB {

  // entries to use eventually:
  // 'shut down' -> 'OK' to check for regular shutdown on last access, remove key when opening database
  // 'database version' -> '1.0'
  // 'hash algorithm' -> 'MD5', 'SHA-1' or similar
  // 'print length' -> ?
  
  def createTables(connection: Connection) : Unit =
    execUpdate(connection, """
      CREATE CACHED TABLE RepositoryInfo (
        key VARCHAR(32) PRIMARY KEY,
        value VARCHAR(256) NOT NULL
      );
    """)

  def dropTables(connection: Connection) : Unit =
    execUpdate(connection, "DROP TABLE RepositoryInfo IF EXISTS;")
    
  def read(connection: Connection) : StringMap =
    execQuery(connection, "SELECT key, value FROM RepositoryInfo;")(result => (result string 1, result string 2)) toMap

  def set(connection: Connection, key: String, value: String) : Unit =
    execUpdate(connection, "INSERT INTO RepositoryInfo (key, value) VALUES (?, ?);", key, value)
    
  def setOrUpdate(connection: Connection, key: String, value: String) : Unit = {
    delete(connection, key)
    set(connection, key, value)
  }

  def delete(connection: Connection, key: String) : Unit =
    execUpdate(connection, "DELETE FROM RepositoryInfo WHERE key = ?;", key)
  
}

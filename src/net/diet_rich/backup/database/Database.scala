// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.database

import java.sql._

class Database {

  Class.forName("org.hsqldb.jdbc.JDBCDriver")
  val connection = DriverManager.getConnection("jdbc:hsqldb:mem:mymemdb", "SA", "")
  val statement = connection.createStatement

  // requests would be for:
  // (size & headerChecksum) => Boolean
  // (size & headerChecksum & hash) => Option[pk]
  // (markedDeleted) => ???
  // (data file) => ???
  // the data location columns will be something like
  // partNumber ; fileID ; locationInFile ; length

  def createTables = {
    statement.execute(
        "CREATE TABLE dataentries ("
        + "pk BIGINT IDENTITY," // entry key
        + "size BIGINT," // entry size
        + "header BIGINT," // header checksum
        + "hash BINARY(32)," // entry hash FIXME size depends on hash algorithm
        + "UNIQUE (size, header, hash)"// FIXME only use for debugging (slower?)
        + ")"
        )
    statement.execute(
        "CREATE INDEX dataentries_size ON dataentries (size)"
        )
    statement.execute(
        "CREATE INDEX dataentries_header ON dataentries (header)"
        )
    statement.execute(
        "CREATE INDEX dataentries_hash ON dataentries (hash)"
        )
    statement.execute(
        "CREATE TABLE datachunks ("
        + "key BIGINT," // entry key
        + "part INTEGER," // serial number of entry part
        + "size BIGINT," // entry part size
        + "fileid BIGINT," // data file ID
        + "location INTEGER," // location in data file
        + "deleted BOOLEAN," // deleted flag
        + "FOREIGN KEY (key) REFERENCES dataentries(pk)" // FIXME only use for debugging (slower?)
        + ")"
        )
  }
  
}
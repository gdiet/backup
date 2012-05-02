// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.sb

import java.sql.Connection
import java.sql.SQLException
import net.diet_rich.util.Configuration._
import net.diet_rich.util.EventSource
import net.diet_rich.util.Events
import net.diet_rich.util.sql._

class ByteStoreSqlDB(protected val connection: Connection) extends SqlDBCommon {
  implicit val con = connection
  
}

object ByteStoreSqlDB extends SqlDBObjectCommon {
  override val tableName = "ByteStore"
  
  def createTable(connection: Connection, repoSettings: StringMap) : Unit = {
    val zeroByteHash = HashProvider.digester(repoSettings).digest
    // index: data part index (starts at 0)
    // start: data part start position
    // fin: data part end position + 1
    execUpdate(connection, """
      CREATE CACHED TABLE ByteStore (
        dataid BIGINT NOT NULL,
        index  INTEGER NOT NULL,
        start  BIGINT NOT NULL,
        fin    BIGINT NOT NULL
      );
    """);
    execUpdate(connection, "CREATE INDEX idxStart ON ByteStore(start);")
    execUpdate(connection, "CREATE INDEX idxFin ON ByteStore(fin);")
  }
  
  // used as index usage markers
  def idxStart[T](t : T) = t
  def idxFin[T](t : T) = t
  
  override protected val internalConstraints = List(
    "UniqueStart UNIQUE (start)",
    "UniqueFin UNIQUE (fin)",
    "ValidPositions CHECK (fin > start AND start >= 0)"
  )
  
  override protected val externalConstraints = List(
    "DataReference FOREIGN KEY (dataid) REFERENCES DataInfo(id)"
  )
  
  def apply(connection: Connection) : DataInfoSqlDB = new DataInfoSqlDB(connection)
}
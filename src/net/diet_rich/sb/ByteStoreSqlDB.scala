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
import ByteStoreSqlDB._

class ByteStoreSqlDB(protected val connection: Connection) extends SqlDBCommon {
  implicit val con = connection

  execQuery(connection, idxStart( idxFin(
    "SELECT b1.start FROM BYTESTORE b1 LEFT JOIN BYTESTORE b2 ON b1.start = b2.fin WHERE b2.fin IS NULL"
  )))(result => result long 1).toList filterNot (_ == 0)
  
}

object ByteStoreSqlDB extends SqlDBObjectCommon {
  override val tableName = "ByteStore"

  // EVENTUALLY, it would be good to look for illegal overlaps:
  // SELECT * FROM ByteStore b1 JOIN ByteStore b2 ON b1.start < b2.fin AND b1.fin > b2.fin
  // Illegal overlaps should be ignored during free space detection

  // EVENTUALLY, it would be good to look for orphan ByteStore entries (in case the FOREIGN KEY constraint is not enabled).
  // Emit warning and free the space?
    
  // Find gaps
  // a) start without matching fin
  // SELECT * FROM BYTESTORE b1 LEFT JOIN BYTESTORE b2 on b1.start = b2.fin where b2.fin is null
  // b) fin without matching start
  // SELECT * FROM BYTESTORE b1 LEFT JOIN BYTESTORE b2 on b1.fin = b2.start where b2.start is null

  // Find highest entry
  // SELECT MAX(fin) FROM ByteStore
    
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
    execUpdate(connection, "CREATE INDEX idxData ON ByteStore(dataid);")
  }
  
  // used as index usage markers
  def idxStart[T](t : T) = t
  def idxFin[T](t : T) = t
  def idxData[T](t : T) = t
  
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
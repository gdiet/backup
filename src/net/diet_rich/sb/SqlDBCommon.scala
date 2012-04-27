package net.diet_rich.sb

import java.sql.Connection
import net.diet_rich.util.ScalaThreadLocal
import net.diet_rich.util.sql._
import java.util.concurrent.atomic.AtomicLong
import java.sql.PreparedStatement

protected trait SqlDBCommon {
  import SqlDBCommon._
  protected def connection: Connection
  
  protected def prepare(statement: String) : ScalaThreadLocal[PreparedStatement] =
    ScalaThreadLocal(connection prepareStatement statement, statement)
    
  protected def readAsAtomicLong(statement: String): AtomicLong = new AtomicLong(
    execQuery(connection, statement)(_ long 1) headOnly
  )

  protected def prepareQuery(statement: String) : Query =
    new Query {
      protected val prepared =
        ScalaThreadLocal(connection prepareStatement statement, statement)
      override def apply[T](args: Any*)(processor: WrappedSQLResult => T): ResultIterator[T] =
        execQuery(prepared, args:_*)(processor)
    }

  protected def prepareUpdate(statement: String) : Update =
    new Update {
      protected val prepared =
        ScalaThreadLocal(connection prepareStatement statement, statement)
      override def apply(args: Any*): Int =
        execUpdate(prepared, args:_*)
    }
  
  protected def throwIllegalUpdateException(where: String, count: Int, id: Long) =
    throw new IllegalStateException("%s: Unexpected %s times update for id %s" format(where, count, id))
}

object SqlDBCommon {
  trait Query {
    def apply[T](args: Any*)(processor: WrappedSQLResult => T): ResultIterator[T]
  }
  trait Update {
    def apply(args: Any*): Int
  }
}

protected trait SqlDBObjectCommon {
  protected def tableName: String
  protected def externalConstraints: List[String] = List()
  protected def internalConstraints: List[String] = List()
  
  final def addConstraints(connection: Connection) : Unit = {
    addInternalConstraints(connection)
    addConstraints(connection, externalConstraints)
  }

  protected def addConstraints(connection: Connection, constraints: List[String]) : Unit =
    constraints foreach(constraint => execUpdate(connection, "ALTER TABLE %s ADD CONSTRAINT %s;" format(tableName, constraint)))
  
  final def addInternalConstraints(connection: Connection) : Unit = 
    addConstraints(connection, internalConstraints)

  final def removeConstraints(connection: Connection, constraints: List[String]) : Unit =
    constraints foreach(constraint => execUpdate(connection,
      "ALTER TABLE %s DROP CONSTRAINT %s;" format(tableName, constraint split " " head)))
  
  final def removeConstraints(connection: Connection) : Unit = {
    removeConstraints(connection, internalConstraints)
    removeConstraints(connection, externalConstraints)
  }
  
  def dropTable(connection: Connection) : Unit =
    execUpdate(connection, "DROP TABLE %s IF EXISTS;" format tableName)
}

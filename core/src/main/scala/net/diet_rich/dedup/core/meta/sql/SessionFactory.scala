package net.diet_rich.dedup.core.meta.sql

import java.io.File
import java.util.concurrent.{ConcurrentLinkedQueue => SynchronizedQueue}

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

import net.diet_rich.dedup.util._
import net.diet_rich.dedup.util.io._

case class SessionFactory(database: CurrentDatabase, writable: Writable) extends AutoCloseable {
  private val allSessions = new SynchronizedQueue[CurrentSession]()
  private val sessions = new ThreadLocal[CurrentSession] {
    override def initialValue = init(database createSession()){allSessions add}
  }
  def session: CurrentSession = sessions.get()
  def close() = {
    session.conn prepareStatement (if (writable) "SHUTDOWN DEFRAG;" else "SHUTDOWN;") execute()
    allSessions.asScala foreach (_ close())
  }
}

object SessionFactory {
  def productionDB(dbFolder: File, writable: Writable) = SessionFactory(
    scala.slick.driver.H2Driver.simple.Database forURL (
      // MV_STORE and MVCC disabled, see http://code.google.com/p/h2database/issues/detail?id=542
      url = s"jdbc:h2:${dbFolder/"dedup"};DB_CLOSE_ON_EXIT=FALSE${if (writable) "" else ";ACCESS_MODE_DATA=r"};MV_STORE=FALSE;MVCC=FALSE",
      user = "sa", driver = "org.h2.Driver"
    ),
    writable
  )
}

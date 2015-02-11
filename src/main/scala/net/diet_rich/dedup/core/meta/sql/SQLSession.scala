package net.diet_rich.dedup.core.meta.sql

import java.io.File
import java.util.concurrent.{ConcurrentLinkedQueue => SynchronizedQueue}

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

import net.diet_rich.dedup.util._
import net.diet_rich.dedup.util.io._

case class SQLSession(database: CurrentDatabase) {
  private val allSessions = new SynchronizedQueue[CurrentSession]()
  private val sessions = new ThreadLocal[CurrentSession] {
    override def initialValue = init(database createSession()){allSessions add}
  }
  def session: CurrentSession = sessions.get()
  def close() = allSessions.asScala foreach (_ close())
}

object SQLSession {
  def withH2(dbFolder: File, readonly: Boolean) = SQLSession(
    scala.slick.driver.H2Driver.simple.Database forURL (
      // MV_STORE and MVCC disabled, see http://code.google.com/p/h2database/issues/detail?id=542
      url = s"jdbc:h2:${dbFolder/"dedup"}${if (readonly) ";ACCESS_MODE_DATA=r" else ""};MV_STORE=FALSE;MVCC=FALSE",
      user = "sa", driver = "org.h2.Driver"
    )
  )
}

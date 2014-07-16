// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.sql

import java.io.File
import java.util.concurrent.{ConcurrentLinkedQueue => SynchronizedQueue}

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

import net.diet_rich.dedup.core.Lifecycle
import net.diet_rich.dedup.util.{init, ThreadSpecific}

trait DatabaseSlice {
  def database: CurrentDatabase
}

trait SessionSlice {
  implicit def session: CurrentSession
}

trait ThreadSpecificSessionsPart extends SessionSlice with DatabaseSlice with Lifecycle {
  private val allSessions = new SynchronizedQueue[CurrentSession]()
  private val sessions = ThreadSpecific(init(database createSession){allSessions add})
  implicit final def session: CurrentSession = sessions.threadInstance
  abstract override def teardown() = {
    super.teardown()
    allSessions.asScala foreach (_ close())
  }
}

object ProductionDatabase {
  import scala.slick.driver.H2Driver.simple.Database

  def fromFile(file: File, readonly: Boolean): CurrentDatabase =
    Database forURL (
      url = s"jdbc:h2:$file${if (readonly) ";ACCESS_MODE_DATA=r" else ""}",
      user = "sa",
      password = "",
      driver = "org.h2.Driver"
    )
}

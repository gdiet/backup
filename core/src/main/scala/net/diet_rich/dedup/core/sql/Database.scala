// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.sql

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

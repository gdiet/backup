// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.sql

import java.io.File
import java.util.concurrent.{ConcurrentLinkedQueue => SynchronizedQueue}

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

import net.diet_rich.dedup.core.Lifecycle
import net.diet_rich.dedup.util.{Memory, init, ThreadSpecific}

trait DatabaseSlice extends Lifecycle {
  import DatabaseSlice.memoryToReserve
  def database: CurrentDatabase
  abstract override def setup() = {
    super.setup()
    require(Memory.reserve(memoryToReserve).isInstanceOf[Memory.Reserved], s"could not reserve $memoryToReserve bytes of memory for database etc.")
  }
  abstract override def teardown() = {
    super.teardown()
    Memory free memoryToReserve
  }
}

object DatabaseSlice {
  private val memoryToReserve = 0x2000000 // 0x2000000 = 32 MB
}

trait SessionSlice {
  implicit def session: CurrentSession
}

trait ThreadSpecificSessionsPart extends SessionSlice with DatabaseSlice {
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
      // MV_STORE and MVCC disabled, see http://code.google.com/p/h2database/issues/detail?id=542
      url = s"jdbc:h2:$file${if (readonly) ";ACCESS_MODE_DATA=r" else ""};MV_STORE=FALSE;MVCC=FALSE",
      user = "sa",
      password = "",
      driver = "org.h2.Driver"
    )
}

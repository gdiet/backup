// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.database

import net.diet_rich.dedup.repository.Repository
import net.diet_rich.util.sql._
import net.diet_rich.util.vals._

trait SettingsDB {
  implicit val connection: WrappedConnection

  def checkDbVersion: Unit =
    assume(
      dbSettings.get(Repository.dbVersionKey) == Some(Repository.dbVersion),
      s"Expected database version ${Repository.dbVersion} but found ${dbSettings.get(Repository.dbVersionKey).getOrElse("None")}."
    )
  
  def dbSettings: Map[String, String] =
    queryEntries(){r => (r string 1, r string 2)} toMap
  protected val queryEntries =
    prepareQuery("SELECT key, value FROM Settings")

  def writeDbSettings(settings: Map[String, String]): Unit = {
    deleteEntries()
    settings.foreach(entry => insertEntries(entry._1, entry._2))
  }
  protected val insertEntries =
    prepareUpdate("INSERT INTO Settings (key, value) VALUES (?, ?)")
  protected val deleteEntries =
    prepareUpdate("DELETE FROM Settings")
}

object SettingsDB {
  def createTable(initialSettings: Map[String, String])(implicit connection: WrappedConnection) : Unit = {
    execUpdate(net.diet_rich.util.Strings normalizeMultiline """
      CREATE TABLE Settings (
        key    VARCHAR(256) PRIMARY KEY,
        value  VARCHAR(256)
      )
    """)
    val insertEntries =
      prepareUpdate("INSERT INTO Settings (key, value) VALUES (?, ?)")
    initialSettings.foreach(entry => insertEntries(entry._1, entry._2))
  }
}

// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.database

import java.sql.Connection
import net.diet_rich.dedup.repository.Repository
import net.diet_rich.util.sql._
import net.diet_rich.util.vals._

object SettingsDB {
  def createTable(initialSettings: Map[String, String])(implicit connection: Connection): Unit = {
    update(net.diet_rich.util.Strings normalizeMultiline """
      CREATE TABLE Settings (
        key    VARCHAR(256) PRIMARY KEY,
        value  VARCHAR(256)
      )
    """)
    writeDbSettings(initialSettings)
  }
  
  def readDbSettings(implicit connection: Connection): Map[String, String] =
    query("SELECT key, value FROM Settings"){r => (r string 1, r string 2)} toMap

  def writeDbSettings(settings: Map[String, String])(implicit connection: Connection): Unit = {
    update("DELETE FROM Settings")
    settings.foreach(entry => update("INSERT INTO Settings (key, value) VALUES (?, ?)", entry._1, entry._2))
  }
  
}

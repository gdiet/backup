// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup

import akka.config.Configuration

object BackupSystemConfig {
  
  // actors.MailboxSizeLimit
  // processing.ChunkSize

  val config : Configuration = Configuration.fromString("")
  
  def apply(): Configuration = config
  
}
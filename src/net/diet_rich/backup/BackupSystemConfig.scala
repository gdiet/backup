package net.diet_rich.backup

import akka.config.Configuration

object BackupSystemConfig {

  val config : Configuration = Configuration.fromString("")
  
  def apply(): Configuration = config
  
}
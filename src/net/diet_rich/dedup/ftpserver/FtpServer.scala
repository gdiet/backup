// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.ftpserver

import net.diet_rich.dedup.CmdLine._
import net.diet_rich.util.CmdApp
import net.diet_rich.dedup.repository.Repository

object FtpServer extends CmdApp {

  def main(args: Array[String]): Unit = run(args)(backup)
  
  val usageHeader = "Runs an FTP server for the dedup repository, account user/user. "
  val paramData = Seq(
    REPOSITORY -> "" -> "[%s <directory>] Mandatory: Repository location"
  )

  def backup(opts: Map[String, String]): Unit = {
    require(! opts(REPOSITORY).isEmpty, s"Repository location setting $REPOSITORY is mandatory.")
    val repository = new Repository(new java.io.File(opts(REPOSITORY)))
    val server = MinaWrapper.server(repository)
    server.start()
  }
  
}
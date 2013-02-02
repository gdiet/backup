// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.ftpserver

import net.diet_rich.dedup.CmdLine._
import net.diet_rich.util.CmdApp
import net.diet_rich.dedup.repository.Repository

object FtpServer extends CmdApp {

  def main(args: Array[String]): Unit = run(args)
  
  val usageHeader = "Runs an FTP server for the dedup repository, account user/user."
  val keysAndHints = Seq(
    REPOSITORY -> "" -> "[%s <directory>] Repository location"
  )

  protected def application(opts: Map[String, String]): Unit = {
    val repository = new Repository(new java.io.File(opts(REPOSITORY)))
    val server = MinaWrapper.server(repository)
    server.start()
  }
  
}
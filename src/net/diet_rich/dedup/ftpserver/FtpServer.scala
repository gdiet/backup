// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.ftpserver

import net.diet_rich.dedup.CmdLine._
import net.diet_rich.util._
import net.diet_rich.dedup.repository.Repository

object FtpServer extends CmdApp {

  def main(args: Array[String]): Unit = run(args)
  
  val usageHeader = "Runs an FTP server for the dedup repository, account user/user."
  val keysAndHints = Seq(
    REPOSITORY -> "" -> "[%s <directory>] Repository location"
  )

  protected def application(con: Console, opts: Map[String, String]): Unit = {
    con.println("Starting a read-only FTP server for the backup repository.")
    con.println("Access: ftp://localhost")
    con.println("User: 'user', password: 'user'")
    val repository = new Repository(new java.io.File(opts(REPOSITORY)), false) // FIXME make read-only optional
    val server = MinaWrapper.server(repository)
    server.start()
    con.println("Server started.")
    
    val shutdownHook = sys.ShutdownHookThread {
      server.stop()
      repository.shutdown(false)
      con.println("Server stopped.")
    }
    
    while (con.readln("Enter 'x' to stop the server: ") != "x") {}
    shutdownHook.remove
    shutdownHook.run
  }
  
}
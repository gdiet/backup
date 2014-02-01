// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.ftpserver

import net.diet_rich.dedup.CmdLine._
import net.diet_rich.util._
import net.diet_rich.dedup.repository.Repository

object FtpServer extends CmdApp {

  val FTPPORT = "-p"
  
  def main(args: Array[String]): Unit = run(args)
  
  val usageHeader = "Runs an FTP server for the dedup repository, account user/user."
  val keysAndHints = Seq(
    REPOSITORY -> "" -> "[%s <directory>] Repository location"
  )
  override protected val optionalKeysAndHints = Seq(
    WRITEPROTECTED -> "y" -> "[%s [y|n]] If not 'n', access is read-only, default '%s'",
    FTPPORT -> "21" -> "[%s [port number]], default '%s'"
  )

  private var closeOperation: () => Boolean = () => false
  
  override protected def closeRequested: Boolean = closeOperation()
  
  protected def application(con: Console, opts: Map[String, String]): Unit = {
    val ftpPort = opts(FTPPORT) toInt
    val readonly = opts(WRITEPROTECTED) != "n"
    if (readonly)
      con.println("Starting a read-only FTP server.")
    else
      con.println("Starting an FTP server with write access.")
    con.println(s"Backup repository: ${opts(REPOSITORY)}")
    con.println(s"Access: ftp://localhost${if (ftpPort == 21) "" else s":$ftpPort"}")
    con.println("User: 'user', password: 'user'")
    val repository = new Repository(new java.io.File(opts(REPOSITORY)), readonly)
    val server = MinaWrapper.server(repository, ftpPort)
    server.start()
    con.println("Server started.")
    
    val shutdownHook = sys.ShutdownHookThread {
      server.stop()
      repository.shutdown(false)
      con.println("Server stopped.")
    }

    closeOperation = synchronized { () =>
      closeOperation = () => false
      true
    }
    
    try { while (con.readln("Enter 'x' to stop the server: ") != "x") {} }
    catch { case e: InterruptedException => () }
    finally {
      shutdownHook.remove
      shutdownHook.run
    }
  }
  
}
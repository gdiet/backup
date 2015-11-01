package net.diet_rich.dedupfs.ftpserver

import java.io.File
import scala.io.StdIn

import org.apache.ftpserver.FtpServerFactory
import org.apache.ftpserver.ftplet.{User, FileSystemFactory}
import org.apache.ftpserver.listener.ListenerFactory
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory
import org.apache.ftpserver.usermanager.impl.BaseUser

import net.diet_rich.common._, io._
import net.diet_rich.dedupfs.{FileSystem, Repository, StoreMethod}

object Main extends App with Logging {
  val arguments = new Arguments(args, 1)
  val List(repoPath) = arguments.parameters
  val ftpPort = arguments intOptional "port" getOrElse 21
  val writable = arguments booleanOptional "writable" getOrElse false
  val storeMethod = arguments optional "storeMethod" map StoreMethod.named getOrElse StoreMethod.STORE
  val versionComment = if (writable) arguments optional "comment" else None
  arguments withSettingsChecked {
    val directory = new File(repoPath)
    val repository = if (writable) Repository openReadWrite(directory, storeMethod) else Repository openReadOnly directory
    using(new FileSystem(repository)) { fileSystem =>
      val fileSystemViewFactory = () => new FileSysView(fileSystem)
      val listener = init(new ListenerFactory()) { _ setPort ftpPort } createListener()
      val fileSystemFactory = new FileSystemFactory { override def createFileSystemView (user: User) = fileSystemViewFactory() }
      val userManager = init(new PropertiesUserManagerFactory() createUserManager()) {
        _ save init(new BaseUser()){user => user setName "user"; user setPassword "user"}
      }
      val server = init(new FtpServerFactory()) { serverFactory =>
        serverFactory addListener ("default", listener)
        serverFactory setFileSystem fileSystemFactory
        serverFactory setUserManager userManager
      } createServer()
      server.start()

      log info s"Started dedup ftp server at ftp://localhost${if (ftpPort == 21) "" else s":$ftpPort"}"
      log info s"Write access is ${if (writable) "ENABLED" else "OFF"}"
      log info s"User: 'user', password: 'user'"
      sys addShutdownHook {
        log info s"Dedup ftp server stopping..."
        server stop()
        fileSystem close()
      }
      System.err.println("Enter 'exit' to exit.")
      while(StdIn.readLine() != "exit") { }
      System.exit(0)
    }
  }
}

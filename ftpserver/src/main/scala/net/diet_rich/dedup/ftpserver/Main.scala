package net.diet_rich.dedup.ftpserver

import java.util.concurrent.CountDownLatch

import org.apache.ftpserver.{FtpServer, FtpServerFactory}
import org.apache.ftpserver.ftplet.{User, FileSystemFactory}
import org.apache.ftpserver.listener.ListenerFactory
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory
import org.apache.ftpserver.usermanager.impl.BaseUser

import net.diet_rich.dedup.core.{RepositoryReadOnly, Repository}
import net.diet_rich.dedup.core.data.StoreMethod
import net.diet_rich.dedup.util._
import net.diet_rich.dedup.util.io._

object Main extends App {
  if (args.length < 2) println("arguments: <command> <repository> [key:value options]") else {
    val commandLineUtils = CommandLineUtils(args)
    import commandLineUtils._
    require(command == "ftpserver")

    val writable = optional("writable") getOrElse "false" toBoolean
    val ftpPort = intOptional("port") getOrElse 21
    val parallel = intOptional("parallel")
    val storeMethod = optional("storeMethod") map StoreMethod.named
    val maxBytesToCache = intOptional("maxBytesToCache") getOrElse 250000000
    val versionComment = optional("versionComment")

    if (writable)
      serverLifeCycle(Repository openReadWrite (repositoryDir, storeMethod, parallel, versionComment))
    else
      serverLifeCycle(Repository openReadOnly repositoryDir)

    def serverLifeCycle[R <: RepositoryReadOnly](repository: R) {
      val server = ftpServer(FileSysView(repository, maxBytesToCache))
      println(s"started dedup ftp server at ftp://localhost${if (ftpPort == 21) "" else s":$ftpPort"}")
      println("write access is " + (if (writable) "ENABLED" else "OFF"))
      println("User: 'user', password: 'user'")
      sys.ShutdownHookThread {
        println("dedup ftp server stopping...")
        server.stop()
        repository.close()
        println("dedup ftp server stopped.")
      }
      Thread sleep Long.MaxValue
    }

    def ftpServer(fileSysView: FileSysView[_ <: RepositoryReadOnly]): FtpServer = {
      val listener = init(new ListenerFactory()) { _ setPort ftpPort } createListener()
      val fileSystemFactory = new FileSystemFactory { override def createFileSystemView (user: User) = fileSysView }
      val userManager = init(new PropertiesUserManagerFactory() createUserManager()) {
        _ save init(new BaseUser()){user => user setName "user"; user setPassword "user"}
      }
      val server = init(new FtpServerFactory()) { serverFactory =>
        serverFactory addListener ("default", listener)
        serverFactory setFileSystem fileSystemFactory
        serverFactory setUserManager userManager
      } createServer()

      init(server)(_ start())
    }
  }
}

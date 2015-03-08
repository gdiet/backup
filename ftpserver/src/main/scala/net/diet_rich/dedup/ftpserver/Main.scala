package net.diet_rich.dedup.ftpserver

import java.util.concurrent.CountDownLatch

import org.apache.ftpserver.FtpServerFactory
import org.apache.ftpserver.ftplet.{User, FileSystemFactory}
import org.apache.ftpserver.listener.ListenerFactory
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory
import org.apache.ftpserver.usermanager.impl.BaseUser

import net.diet_rich.dedup.core.Repository
import net.diet_rich.dedup.core.data.StoreMethod
import net.diet_rich.dedup.util._
import net.diet_rich.dedup.util.io._

object Main extends App {
  if (args.length < 2) println("arguments: <command> <repository> [key:value options]") else new CommandLineUtils(args) {
    require(command == "ftpserver")

    val writable = optional("writable")
    val ftpPort = intOptional("port") getOrElse 21
    val parallel = intOptional("parallel")
    val storeMethod = optional("storeMethod") map StoreMethod.named

    using(Repository readOnly repositoryDir) { repository =>
      val listener = init(new ListenerFactory()) {
        _ setPort ftpPort
      } createListener()
      val fileSystemFactory = new FileSystemFactory {
        override def createFileSystemView (user: User) = FileSysViewReadOnly(repository)
      }
      val userManager = init(new PropertiesUserManagerFactory() createUserManager()) {
        _ save init(new BaseUser()){user => user setName "user"; user setPassword "user"}
      }
      val server = init(new FtpServerFactory()) { serverFactory =>
        serverFactory addListener ("default", listener)
        serverFactory setFileSystem fileSystemFactory
        serverFactory setUserManager userManager
      } createServer()

      server.start()
      println(s"started dedup ftp server at ftp://localhost${if (ftpPort == 21) "" else s":$ftpPort"}")
      println("User: 'user', password: 'user'")

      val latch = new CountDownLatch(1)
      val shutdownHook = sys.ShutdownHookThread {
        println("dedup ftp server stopping...")
        server.stop()
        latch.countDown()
      }
      latch.await()
    }
    println("dedup ftp server stopped.")
  }
}

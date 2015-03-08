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
  if (args.length < 2) println("arguments: <command> <repository> [key:value options]") else {
    val commandLineUtils = CommandLineUtils(args)
    import commandLineUtils._
    require(command == "ftpserver")

    val writable = optional("writable") getOrElse "false" toBoolean
    val ftpPort = intOptional("port") getOrElse 21
    val parallel = intOptional("parallel")
    val storeMethod = optional("storeMethod") map StoreMethod.named
    val maxBytesToCache = intOptional("maxBytesToCache") getOrElse 250000000

    if (writable)
      using(Repository readWrite (repositoryDir, storeMethod, parallel)) { repository =>
        run(FileSysView(repository, Some(repository), maxBytesToCache))
      }
    else
      using(Repository readOnly repositoryDir) { repository =>
        run(FileSysView(repository, None, maxBytesToCache))
      }
    println("dedup ftp server stopped.")

    def run(fileSysView: FileSysView) = {
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

      server.start()
      println(s"started dedup ftp server at ftp://localhost${if (ftpPort == 21) "" else s":$ftpPort"}")
      println("write access is " + (if (writable) "ENABLED" else "OFF"))
      println("User: 'user', password: 'user'")

      val latch = new CountDownLatch(1)
      val shutdownHook = sys.ShutdownHookThread {
        println("dedup ftp server stopping...")
        server.stop()
        latch.countDown()
      }
      latch.await()
    }
  }
}

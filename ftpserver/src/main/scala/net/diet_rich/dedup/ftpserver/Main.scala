package net.diet_rich.dedup.ftpserver

import org.apache.ftpserver.FtpServerFactory
import org.apache.ftpserver.ftplet.{User, FileSystemFactory}
import org.apache.ftpserver.listener.ListenerFactory
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory
import org.apache.ftpserver.usermanager.impl.BaseUser

import net.diet_rich.dedup.core.Repository
import net.diet_rich.dedup.core.data.StoreMethod
import net.diet_rich.dedup.util._

object Main extends App with Logging {
  if (args.length < 2) println("arguments: <command> <repository> [key:value options]") else {
    val commandLineUtils = CommandLineUtils(args)
    import commandLineUtils._
    require(command == "ftpserver")

    val writable = optional("writable") getOrElse "false" toBoolean
    val ftpPort = intOptional("port") getOrElse 21

    val (repository, fileSystemViewFactory) =
      if (writable) {
        val storeMethod = optional("storeMethod") map StoreMethod.named
        val parallel = intOptional("parallel")
        val maxBytesToCache = intOptional("maxBytesToCache") getOrElse 250000000
        val versionComment = optional("versionComment")
        val repository = Repository openReadWrite (repositoryDir, storeMethod, parallel, versionComment)
        (repository, () => FileSysViewReadWrite(repository, maxBytesToCache))
      }
      else {
        val repository = Repository openReadOnly repositoryDir
        (repository, () => FileSysViewReadOnly(repository))
      }

    val listener = init(new ListenerFactory()) { _ setPort ftpPort } createListener()
    val fileSystemFactory = new FileSystemFactory {
      override def createFileSystemView (user: User) = fileSystemViewFactory()
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

    log info s"started dedup ftp server at ftp://localhost${if (ftpPort == 21) "" else s":$ftpPort"}"
    log info s"write access is ${if (writable) "ENABLED" else "OFF"}"
    log info s"User: 'user', password: 'user'"
    sys addShutdownHook {
      log info s"dedup ftp server stopping..."
      server stop()
      repository close()
    }
    Thread sleep Long.MaxValue
  }
}

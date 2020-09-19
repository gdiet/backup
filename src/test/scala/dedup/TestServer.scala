package dedup

import java.io.File

import jnr.ffi.Runtime.getSystemRuntime
import jnr.ffi.provider.jffi.ArrayMemoryIO

object TestServer extends App {
  val targetDir = new File("dedupfs-temp").getAbsoluteFile
  CleanInit.delete(targetDir)
  Server.main(Array("init", s"repo=$targetDir"))
  val server = new Server(targetDir, targetDir, false)
  println("mkdir    " + server.mkdir("/hello", 0))
  println("create   " + server.create("/hello/file.txt", 0, null))
  println("truncate " + server.truncate("/hello/file.txt", 1232388))
  if (true) {
    val data = Array.fill[Byte](262144)(65)
    val buf = new ArrayMemoryIO(getSystemRuntime, data, 0, 262144)
    println("write    " + server.write("/hello/file.txt", buf, 262144, 0, null))
  }
  if (true) {
    val data = Array.fill[Byte](970244)(77)
    val buf = new ArrayMemoryIO(getSystemRuntime, data, 0, 970244)
    println("write    " + server.write("/hello/file.txt", buf, 970244, 262144, null))
  }
  println("release  " + server.release("/hello/file.txt", null))

//  println("write " + server.write("/hello/file.txt", new ArrayMemoryIO(getSystemRuntime, Array[Byte](65, 66, 67, 68, 69), 0, 5), 5, 0, null))
//  def read(): Unit = {
//    val readBuffer = Array.fill[Byte](10)( 99)
//    println("read " + server.read("/hello/file.txt", new ArrayMemoryIO(getSystemRuntime, readBuffer, 0, 10), 7, 1, null))
//    println("read " + readBuffer.mkString(", "))
//  }
//  read()
//  println("truncate " + server.truncate("/hello/file.txt", 6))
//  read()
//  println("release " + server.release("/hello/file.txt", null))
//  println("open " + server.open("/hello/file.txt", null))
//  read()
//  Thread.sleep(500)
//  read()
//  println("release " + server.release("/hello/file.txt", null))
//  println("open " + server.open("/hello/file.txt", null))
//  read()
//  println("release " + server.release("/hello/file.txt", null))
//
  Thread.sleep(500)
  server.umount()
}

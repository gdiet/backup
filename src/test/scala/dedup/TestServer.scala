package dedup

import java.io.File

import jnr.ffi.Runtime.getSystemRuntime
import jnr.ffi.provider.jffi.ArrayMemoryIO

object TestServer extends App {
  val targetDir = new File("dedupfs-temp").getAbsoluteFile
  CleanInit.delete(targetDir)
  Server.main(Array("init", s"repo=$targetDir"))
  val server = new Server(targetDir, targetDir, false)
  println("mkdir " + server.mkdir("/hello", 0))
  println("mkdir " + server.mkdir("/hello/world", 0))
  println("create " + server.create("/hello/file.txt", 0, null))
  println("write " + server.write("/hello/file.txt", new ArrayMemoryIO(getSystemRuntime, Array[Byte](65, 66, 67, 68, 69), 0, 5), 5, 0, null))
  val readBuffer = Array.fill[Byte](10)( 99)
  println("read " + server.read("/hello/file.txt", new ArrayMemoryIO(getSystemRuntime, readBuffer, 0, 10), 7, 1, null))
  println("read " + readBuffer.mkString(", "))
  server.umount()
}

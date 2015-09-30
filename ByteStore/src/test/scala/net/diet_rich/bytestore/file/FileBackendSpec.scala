package net.diet_rich.bytestore.file

import java.io.File

import org.specs2.Specification
import org.specs2.specification.AfterAll

import net.diet_rich.common._, io._

class FileBackendSpec extends Specification with AfterAll {
  args(sequential = true)
  def is = s2"""
Tests for the byte store file backend, starting with an empty store

Read from the empty store
 ${eg{ store.read(0, 1) === Seq(bytes(0)) }}
 ${eg{ 1 === 1 }}
  """

  // FIXME test utilities
  def delete(dir: File): Unit = {
    if (dir.isDirectory) dir.listFiles() foreach delete
    dir.delete()
  }

  val tempDir = new File(System.getProperty("java.io.tmpdir")) / "_specs2_"

  def bytes(b: Byte*) = Bytes(Array(b:_*), 0, b.size)

  import FileBackend._
  lazy val store = {
    val dataDirectory = tempDir / "FileBackendSpec" / "emptyStore"
    delete(dataDirectory)
    dataDirectory.getParentFile.mkdirs()
    initializeDirectory(dataDirectory, "emptyStore", 4)
    readWrite(dataDirectory, "emptyStore")
  }

  override def afterAll(): Unit = store.close()
}

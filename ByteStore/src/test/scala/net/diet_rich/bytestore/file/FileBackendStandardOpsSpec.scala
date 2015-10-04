package net.diet_rich.bytestore.file

import java.io.File

import org.specs2.Specification
import org.specs2.specification.AfterAll

import net.diet_rich.common._, io._

class FileBackendStandardOpsSpec extends Specification with AfterAll {
  args(sequential = true)
  def is = s2"""
Tests for the byte store file backend, starting with an empty store of block size 4

Read from the empty store
 ${eg{ read(0, 1) === List(bytes(0)) }}
 ${eg{ write(bytes(1,2), 3); success }}
 ${eg{ read(2, 6) === List(bytes(0, 1), bytes(2, 0)) }}
 ${eg{ readRaw(3, 9) === List(Right(bytes(1)), Right(bytes(2)), Left(3), Left(1)) }}
  """

  // FIXME test utilities
  def delete(dir: File): Unit = {
    if (dir.isDirectory) dir.listFiles() foreach delete
    dir.delete()
  }

  val tempDir = new File(System.getProperty("java.io.tmpdir")) / "_specs2_"

  def bytes(b: Byte*) = Bytes(Array(b:_*), 0, b.size)

  lazy val store = {
    import FileBackend._
    val dataDirectory = tempDir / "FileBackendSpec" / "emptyStore"
    delete(dataDirectory)
    dataDirectory.getParentFile.mkdirs()
    initializeDirectory(dataDirectory, "emptyStore", 4)
    readWriteRaw(dataDirectory, "emptyStore")
  }

  def read(from: Long, to: Long) = store.read(from, to).toList
  def readRaw(from: Long, to: Long) = store.readRaw(from, to).toList
  def write = store.write _

  override def afterAll(): Unit = store.close()
}

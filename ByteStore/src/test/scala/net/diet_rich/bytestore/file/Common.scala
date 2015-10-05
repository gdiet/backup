package net.diet_rich.bytestore.file

import org.specs2.specification.AfterAll

import net.diet_rich.common._, io._, test._

trait Common extends SpecHelper with AfterAll {
  var store = createStore()
  def createStore() = {
    import FileBackend._
    val dataDirectory = testDataDirectory / "emptyStore"
    delete(dataDirectory)
    dataDirectory.getParentFile.mkdirs()
    initializeDirectory(dataDirectory, "emptyStore", 4)
    openStore()
  }
  def openStore() = {
    import FileBackend._
    val dataDirectory = testDataDirectory / "emptyStore"
    readWriteRaw(dataDirectory, "emptyStore")
  }
  def read(from: Long, to: Long) = store.read(from, to).toList
  def readRaw(from: Long, to: Long) = store.readRaw(from, to).toList
  def write = store.write _
  def clear = store.clear _

  override def afterAll(): Unit = store.close()
}

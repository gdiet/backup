package net.diet_rich.dfs.ds

import org.testng.annotations.{BeforeMethod,Test}
import java.io.File
import org.apache.commons.io.FileUtils
import SpecDataStoreTests._

class SpecDataStoreTests {

  @BeforeMethod
  def prepareTest = {
    FileUtils.cleanDirectory(testDir)
  }
  
  @Test
  def writeAndRead = {
    val store = new BasicDataStore(testDir)
    throw new AssertionError
  }
  
  @Test
  def writeCloseRead = throw new AssertionError
  
  @Test
  def writeOverwriteRead = throw new AssertionError

  @Test
  def writeOverFileLimitRead = throw new AssertionError
  
  @Test
  def writeOverFileLimitCloseRead = throw new AssertionError

  @Test
  def writeAferCloseMustFail = throw new AssertionError

  @Test
  def readAferCloseMustFail = throw new AssertionError
  
}

object SpecDataStoreTests {
  val testDir = new File("temp/net.diet_rich.dfs.ds.SpecDataStoreTests")
  testDir.mkdirs
}
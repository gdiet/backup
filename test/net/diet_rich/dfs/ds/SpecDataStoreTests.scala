package net.diet_rich.dfs.ds

import org.fest.assertions.Assertions.assertThat
import org.testng.annotations.{BeforeMethod,Test}
import java.io.File
import org.apache.commons.io.FileUtils
import SpecDataStoreTests._
import net.diet_rich.util.data.Bytes
import net.diet_rich.util.io.using

class SpecDataStoreTests {

  @BeforeMethod
  def prepareTest = {
    FileUtils.cleanDirectory(testDir)
  }
  
  @Test
  def writeAndRead = using (new BasicDataStore(testDir)) { store =>
    val data = Bytes forLong 987654321
    store write (5000, data)
    assertThat( store read (5000, data length) bytes ) isEqualTo data.bytes
  }
  
  @Test
  def writeCloseRead = {
    val store1 = new BasicDataStore(testDir)
    val data = Bytes forLong 987654321
    store1 write (5000, data)
    store1.close
    using (new BasicDataStore(testDir)) { store2 =>
      assertThat( store2 read (5000, data length) bytes ) isEqualTo data.bytes
    }
  }
  
  @Test
  def moreTests = throw new AssertionError
  
//  @Test
//  def writeOverwriteRead = throw new AssertionError
//
//  @Test
//  def writeOverFileLimitRead = throw new AssertionError
//  
//  @Test
//  def writeOverFileLimitCloseRead = throw new AssertionError
//
//  @Test
//  def writeAferCloseMustFail = throw new AssertionError
//
//  @Test
//  def readAferCloseMustFail = throw new AssertionError
  
}

object SpecDataStoreTests {
  val testDir = new File("temp/net.diet_rich.dfs.ds.SpecDataStoreTests")
  testDir.mkdirs
}
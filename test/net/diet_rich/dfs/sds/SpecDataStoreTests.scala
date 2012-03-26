package net.diet_rich.dfs.sds

import java.io.File
import net.diet_rich.util.Bytes
import net.diet_rich.util.io.using
import org.apache.commons.io.FileUtils
import org.fest.assertions.Assertions.assertThat
import org.testng.annotations.{BeforeMethod,Test}
import SpecDataStoreTests._

class SpecDataStoreTests {

  @BeforeMethod
  def prepareTest = {
    FileUtils.cleanDirectory(testDir)
    assertThat(BasicDataStore.initDirectory(testDir, storeConfig)) isTrue()
  }

  @Test
  def readAcrossFileLimit = {
    val store = BasicDataStore(testDir, systemConfig)
    val data = store .read (990, 20) .toList
    assertThat(data size) isEqualTo(2)
    data foreach {bytes => assertThat(bytes.data) containsOnly(0)}
  }
  
  @Test
  def writeAndReadWithinFileLimit = {
    val store = BasicDataStore(testDir, systemConfig)
    val content = Bytes(8) writeLong(0, 987654321)
    store write (5000, content)
    val data = store .read (5000, content size) .toList
    assertThat(data size) isEqualTo(1)
    assertThat(data.head.copyOfBytes) isEqualTo content.data
  }
  
  @Test
  def writeAndReadAcrossFileLimit = {
    val store = BasicDataStore(testDir, systemConfig)
    val content = Bytes(8) writeLong(0, 987654321)
    store write (996, content)
    val data = store .read (996, content size) .toList
    assertThat(data size) isEqualTo(2)
    assertThat(data(0).copyOfBytes) isEqualTo content.setSize(4).copyOfBytes
    assertThat(data(1).copyOfBytes) isEqualTo content.dropFirst(4).copyOfBytes
  }
  
  @Test
  def writeFlushNewRead = {
    val store1 = BasicDataStore(testDir, systemConfig)
    val content = Bytes(8) writeLong(0, 987654321)
    store1 write (5000, content)
    store1.flush
    val store2 = BasicDataStore(testDir, systemConfig)
    val data = store2 .read (5000, content size) .toList
    assertThat(data size) isEqualTo(1)
    assertThat(data.head.copyOfBytes) isEqualTo content.data
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
  val testDir = new File("temp/net.diet_rich.dfs.sds.SpecDataStoreTests")
  testDir.mkdirs
  val storeConfig = Map("dataLength" -> "1000")
  val systemConfig = Map("openFiles" -> "2")
}
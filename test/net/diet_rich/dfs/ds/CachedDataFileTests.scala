package net.diet_rich.dfs.ds

import org.fest.assertions.Assertions.assertThat
import org.testng.annotations.{BeforeMethod,Test}
import java.io.File
import net.diet_rich.dfs.TestUtil.expectThat
import net.diet_rich.util.data.Bytes
import net.diet_rich.util.io.using
import CachedDataFileTests._
import java.io.DataOutputStream
import java.io.FileOutputStream

class CachedDataFileTests {

  @BeforeMethod
  def prepareTest = testFile.delete
  
  @Test
  def runWithIllegalConstructors = {
    expectThat( new CachedDataFile(0, 0, testFile) ) doesThrow new AssertionError
    expectThat( new CachedDataFile(0, 1, null) ) doesThrow new AssertionError
  }

  @Test
  def runWithExceptionalConstructors = {
    new CachedDataFile(Long.MinValue, 1, testFile)
  }
  
  @Test
  def writeDataWithoutReadDataShouldFail = {
    val cdfile = new CachedDataFile(0, 1, testFile)
    expectThat( cdfile.writeData ) doesThrow new AssertionError
  }

  @Test
  def ignoringCorruptFileLengthWillFailOnWriteData = {
    using(new DataOutputStream(new FileOutputStream(testFile))) { out =>
      out.writeLong(0);
      out.writeLong(10);
      out.writeLong(0);
    }
    val cdfile = new CachedDataFile(0, 5, testFile) { def setDirty = { dirty = true } }
    assertThat( cdfile readData ) isFalse()
    cdfile.setDirty
    expectThat( cdfile writeData ) doesThrow new AssertionError
  }

  @Test
  def noWriteIfNotDirty = {
    val cdfile = new CachedDataFile(0, 5, testFile)
    assertThat( cdfile readData ) isTrue()
    cdfile.writeData
    assertThat( testFile.exists ) isFalse
  }
  
  @Test
  def writeDataToFileAndReadItAgain = {
    val cdfile = new CachedDataFile(0, 100, testFile)
    val content = Bytes("example content" getBytes "UTF-8")
    assertThat (cdfile readData) isTrue()
    assertThat (cdfile write(0, content)) isEqualTo None
    cdfile.writeData
    
    val cdfile2 = new CachedDataFile(0, 100, testFile)
    assertThat (cdfile2 readData) isTrue()
    val read = cdfile2.read(0, content.length)
    assertThat(read toArray) isEqualTo(content toArray)
  }
  
}


object CachedDataFileTests {
  val testFile = new File("temp/net.diet_rich.dfs.ds.CachedDataFileTests/file")
  testFile.getParentFile.mkdirs
}
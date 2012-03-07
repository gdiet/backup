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
    val cdfile = new CachedDataFile(0, 5, testFile)
    expectThat( cdfile.writeData ) doesThrow new AssertionError
  }

  @Test
  def writeDataToFileAndReadItAgain = {
    val cdfile = new CachedDataFile(0, 100, testFile) { def getData = data }
    val content = Bytes("example content" getBytes "UTF-8")
    assertThat (cdfile readData) isTrue()
    assertThat (cdfile write(0, content)) isEqualTo None
    cdfile.writeData
    assert(false) // CONTINUE
  }
  
}


object CachedDataFileTests {
  val testFile = new File("temp/net.diet_rich.dfs.ds.CachedDataFileTests/file")
  testFile.getParentFile.mkdirs
}
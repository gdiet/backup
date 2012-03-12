package net.diet_rich.dfs.ds

import org.fest.assertions.Assertions.assertThat
import org.testng.annotations.{BeforeMethod,Test}
import java.io.File
import net.diet_rich.dfs.TestUtil.expectThat
import net.diet_rich.util.data.Bytes
import net.diet_rich.util.data.Digester
import net.diet_rich.util.io.{using,RandomAccessFile}
import CachedDataFileTests._

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
    val print = Digester.crcadler().writeAnd(Bytes(10)).getDigest
    val content = Bytes(34)
    content store 0 store 10 store print
    using(new RandomAccessFile(testFile)) { _.write(content) }
    val cdfile = new CachedDataFile(0, 5, testFile) { def setDirty = { dirty = true } }
    assertThat( cdfile readData ) isFalse()
    cdfile.setDirty
    expectThat( cdfile writeData ) doesThrow new AssertionError
  }

  @Test
  def intactFileIsReadOK = {
    val print = Digester.crcadler().writeAnd(Bytes(10)).getDigest
    val content = Bytes(34)
    content store 5 store 10 store print
    using(new RandomAccessFile(testFile)) { _.write(content) }
    val cdfile = new CachedDataFile(5, 10, testFile) {def a = all}
    val result = cdfile.readData
    assertThat( cdfile.a.readOffset ) isEqualTo 5
    assertThat( cdfile.a.readLength ) isEqualTo 10
    assertThat( cdfile.a.readPrint ) isEqualTo print
    assertThat( result ) isTrue
  }
  
  @Test
  def corruptPrintWillBeDetectedOnReadData = {
    val content = Bytes(34)
    content store 0 store 10 store 0
    using(new RandomAccessFile(testFile)) { _.write(content) }
    val cdfile = new CachedDataFile(0, 10, testFile) {def a = all}
    val result = cdfile.readData
    assertThat( cdfile.a.readOffset ) isEqualTo 0
    assertThat( cdfile.a.readLength ) isEqualTo 10
    assertThat( cdfile.a.readPrint ) isEqualTo 0
    assertThat( result ) isFalse
  }

  @Test
  def corruptOffsetWillBeDetectedOnReadData = {
    val print = Digester.crcadler().writeAnd(Bytes(10)).getDigest
    val content = Bytes(34)
    content store 0 store 10 store print
    using(new RandomAccessFile(testFile)) { _.write(content) }
    val cdfile = new CachedDataFile(1, 10, testFile) {def a = all}
    val result = cdfile.readData
    assertThat( cdfile.a.readOffset ) isEqualTo 0
    assertThat( cdfile.a.readLength ) isEqualTo 10
    assertThat( cdfile.a.readPrint ) isEqualTo print
    assertThat( result ) isFalse
  }
  
  @Test
  def corruptDataLengthWillBeDetectedOnReadData = {
    val print = Digester.crcadler().writeAnd(Bytes(10)).getDigest
    val content = Bytes(34)
    content store 0 store 5 store print
    using(new RandomAccessFile(testFile)) { _.write(content) }
    val cdfile = new CachedDataFile(0, 10, testFile) {def a = all}
    val result = cdfile.readData
    assertThat( cdfile.a.readOffset ) isEqualTo 0
    assertThat( cdfile.a.readLength ) isEqualTo 5
    assertThat( cdfile.a.readPrint ) isEqualTo print
    assertThat( result ) isFalse
  }
  
  @Test
  def noReadingTwice = {
    val cdfile = new CachedDataFile(0, 5, testFile)
    assertThat( cdfile readData ) isTrue()
    expectThat( cdfile readData ) doesThrow new AssertionError
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
    val cdfile = new CachedDataFile(0, 100, testFile){ def a = all }
    val content = Bytes("example content" getBytes "UTF-8")
    assertThat (testFile exists) isFalse()
    assertThat (cdfile readData) isTrue()
    println(cdfile.a.allData)
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
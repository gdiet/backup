package net.diet_rich.dfs.ds

import org.fest.assertions.Assertions.assertThat
import org.testng.annotations.{BeforeTest,Test}
import java.io.File
import net.diet_rich.util.data.Bytes
import net.diet_rich.dfs.TestUtil.expectThat
import CachedDataFileTests._

class CachedDataFileTests extends CachedDataFile(testOffset, testLength, testFile) {

  @BeforeTest
  def prepareTest = {
    require(!file.exists)
    data = Bytes(0)
    assertThat(readData) isTrue
  }

  @Test
  def cachedDataShouldBeAllZeroIfNothingIsStored = {
    val result = read(testOffset, testLength)
    assertThat(result.bytes) containsOnly 0
  }

  @Test
  def writtenDataShouldBeReadableInDifferentChunks = {
    val content = Bytes("example content" getBytes "UTF-8")
    
    assertThat( write(testOffset + 10, content keepFirst 5) ) isEqualTo None
    assertThat( write(testOffset + 15, content dropFirst 5) ) isEqualTo None

    val part1 = read(testOffset + 10, 3)
    val part2 = read(testOffset + 13, content.length - 3)
    
    val result = Bytes(content.length) copyFrom (part1, 0) copyFrom (part2, 3)
    assertThat(content.bytes) isEqualTo result.bytes // FIXME use Bytes comparison
  }

  @Test
  def writeShouldReturnTheRemainder = {
    val content = Bytes("example content" getBytes "UTF-8")
    val remainder = write(testOffset + testLength - 7, content)
    assertThat(remainder.get toArray) isEqualTo("content" getBytes "UTF-8")
  }
  
  @Test
  def failIfWriteOffsetIsTooSmall =
    expectThat( write(testOffset-1, Bytes(1)) ) doesThrow new AssertionError

  @Test
  def failIfWriteOffsetIsTooLarge =
    expectThat( write(testOffset + testLength, Bytes(1)) ) doesThrow new AssertionError

  @Test
  def failIfReadOffsetIsTooSmall =
    expectThat( read(testOffset-1, 1) ) doesThrow new AssertionError

  @Test
  def failIfReadEndIsToHigh =
    expectThat( read(testOffset, testLength + 1) ) doesThrow new AssertionError

  @Test
  def runWithIllegalConstructors = {
    expectThat( new CachedDataFile(0, 0, new File("")) ) doesThrow new AssertionError
    expectThat( new CachedDataFile(0, 1, null) ) doesThrow new AssertionError
  }

  @Test
  def runWithExceptionalConstructors = {
    new CachedDataFile(Long.MinValue, 1, new File(""))
  }

}


object CachedDataFileTests {
  val testOffset = Int.MaxValue.toLong + 765
  val testLength = 1001
  val testFile = new File("path does not exist / file does not exist")
}
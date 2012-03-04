package net.diet_rich.dfs.ds

import org.fest.assertions.Assertions.assertThat
import org.testng.annotations.{BeforeTest,Test}
import java.io.File
import net.diet_rich.util.data.Bytes
import CachedDataFileTests._

class CachedDataFileTests extends CachedDataFile(testOffset, testLength, testFile) {

  @BeforeTest
  def prepareTest = {
    require(!file.exists)
    data = Bytes(0)
    assertThat(readData) isTrue
  }
  
  @Test
  def writtenDataShouldBeReadableInDifferentChunks = {
    val content = Bytes("example content".getBytes("UTF-8"))
    
    write(testOffset + 10, content.keepFirst(5))
    write(testOffset + 15, content.dropFirst(5))

    val part1 = read(testOffset + 10, 3)
    val part2 = read(testOffset + 13, content.length - 3)
    
    val result = Bytes(content.length)
    result copyFrom (part1, 0)
    result copyFrom (part2, 3)
    assertThat(content.bytes) isEqualTo result.bytes
  }
  
}

object CachedDataFileTests {
  val testOffset = Int.MaxValue.toLong + 765
  val testLength = 1001
  val testFile = new File("path does not exist / file does not exist")
}
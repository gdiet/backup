// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.storelogic

// note: the following imports are useful for mockito matching
//  import org.mockito.Matchers.{argThat,eq => isEq}
//  import org.hamcrest.CoreMatchers._

class StoreLogic_storeOrLink extends net.diet_rich.testutils.TestDataFileProvider {
  import org.fest.assertions.Assertions.assertThat
  import org.testng.annotations.Test
  import org.mockito.Mockito._
  import net.diet_rich.util.data.Digester
  import net.diet_rich.util.data.Digester.Checksum
  import net.diet_rich.util.io.{Closeable,OutputStream,ResettableInputStream}
  import net.diet_rich.util.JStrings._
  import StoreLogic._

  val location = new DataLocation {}
  val storedData = new collection.mutable.ArrayBuffer[Byte]
  
  class StoreStub extends StoreLogic {
    override val headerSize = 10
    override def newHeaderDigester() = Digester.adler32
    override def newHashDigester() = Digester.hash("MD5")
    override def dbContains(size: Long, headerChecksum: Checksum) = false
    override def dbLookup(size: Long, headerChecksum: Checksum, hash: DataHash) : Option[Long] = None
    override def dbStoreLocation(size: Long, headerChecksum: Checksum, hash: DataHash, data: DataLocation) = Left(321)
    override def dbMarkDeleted(data: DataLocation) = { }
    override def newStoreStream() : Digester[DataLocation] with Closeable = 
      new Digester[DataLocation] with Closeable {
        override def write(buffer: Array[Byte], offset: Int, length: Int) =
          storedData ++= buffer.toSeq.slice(offset, offset + length)
        override def getDigest = location
        override def close = {}
      }
  }
  
  @Test
  def blueSky_StoreLogic_storeOrLink {
    val storeMock = spy(new StoreStub)
    
    val input1 = new ResettableInputStream.FromFile(testDataFile("15_A")) with ResettableBackupInput {
      override val sourceForLog : AnyRef = testDataFile("15_A")
      override def length : Long = randomAccessFile.length
    }
    val adler_10_A = Checksum(0x0e01028b)
    val hash_15_A = hex2Bytes("409c94b762769ea5fb9384eb9bddf207")

    // store input 1
    assertThat(storeMock.storeOrLink(input1)) isEqualTo 321

    // check it was processed correctly
    verify(storeMock).dbContains(15, adler_10_A)
    verify(storeMock).dbStoreLocation(15L, adler_10_A, hash_15_A, location)
    assertThat(storedData.sameElements("AAAAAAAAAAAAAAA")).isTrue

    // now, mock the answers for input 1
    when(storeMock.dbContains(15, adler_10_A)) thenReturn true
    when(storeMock.dbLookup(15, adler_10_A, hash_15_A)) thenReturn Option(123L)

    // store input 1 again
    input1.reset
    assertThat(storeMock.storeOrLink(input1)) isEqualTo 123
    
    // check it was processed correctly and no data was written
    verify(storeMock).dbLookup(15, adler_10_A, hash_15_A)
    assertThat(storedData.length) isEqualTo 15
    
  }

}
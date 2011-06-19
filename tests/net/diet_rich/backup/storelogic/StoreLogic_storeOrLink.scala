// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.storelogic

class StoreLogic_storeOrLink extends net.diet_rich.testutils.TestDataFileProvider {
  import org.fest.assertions.Assertions.assertThat
  import org.testng.annotations.Test
  import org.mockito.Mockito._
  import net.diet_rich.util.data.Digester
  import net.diet_rich.util.data.Digester.Checksum
  import net.diet_rich.util.io.{Closeable,OutputStream,ResettableInputStream}
  import StoreLogic._

  @Test
  def blueSky_StoreLogic_storeOrLink {
     class StoreStub extends StoreLogic {
      override val headerSize = 10
      override def newHeaderDigester() = Digester.adler32
      override def newHashDigester() = Digester.hash("MD5")
      override def dbContains(size: Long, headerChecksum: Checksum) = false
      override def dbLookup(size: Long, headerChecksum: Checksum, hash: DataHash) : Option[DataLocation] = None
      override def dbMarkDeleted(data: DataLocation) = { }
      override def newStoreStream() : Digester[DataLocation] with Closeable = 
        new Digester[DataLocation] with Closeable {
          def write(buffer: Array[Byte], offset: Int, length: Int) : Unit = { }
          def getDigest : DataLocation = new DataLocation {}
          def close : Unit = {}
        }
    }
    val storeMock = spy(new StoreStub)
    
    val input1 = new ResettableInputStream.FromFile(testDataFile("15_A")) with ResettableBackupInput {
      override val sourceForLog : AnyRef = testDataFile("15_A")
      override def length : Long = randomAccessFile.length
    }
    val adler_10_A = Checksum(0x0e01028b)
    
    when(storeMock.dbContains(15, adler_10_A)).thenThrow(new AssertionError("hit"))

    storeMock.storeOrLink(input1)

    verify(storeMock).dbContains(15, adler_10_A)

  }

}
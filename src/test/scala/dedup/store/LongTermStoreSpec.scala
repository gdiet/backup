package dedup
package store

class LongTermStoreSpec extends org.scalatest.freespec.AnyFreeSpec with TestFile with org.scalatest.BeforeAndAfterAll:
  require(testFile.mkdirs(), "testFile.mkdirs returned false")
  val lts = LongTermStore(testFile, false)

  override def afterAll(): Unit = lts.close()

  "Attempts to read from missing data files yield zeros" in
    assert(lts.read(1000, 5, 200)._seq == Seq(200 -> Seq(0,0,0,0,0)))

  "Reading across a data file boundary (missing files) results in two data chunks" in
    assert(lts.read(lts.fileSize - 2, 4, 200)._seq == Seq(200 -> Seq(0,0), 202 -> Seq(0,0)))

  "Writing across a data file boundary is possible" in
    lts.write(lts.fileSize - 2, Array[Byte](2,3,4,5))

  "Reading across a data file boundary (existing files, second too short) results in two data chunks" in
    assert(lts.read(lts.fileSize - 2, 6, 200)._seq == Seq(200 -> Seq(2,3), 202 -> Seq(4,5,0,0)))

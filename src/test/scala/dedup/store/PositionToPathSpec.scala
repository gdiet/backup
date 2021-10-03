package dedup
package store

class PositionToPathSpec extends org.scalatest.freespec.AnyFreeSpec:
  "Negative position, e.g. -1, yields an exception" in
    intercept[IllegalArgumentException](pathOffsetSize(-1,0))
  "Path of position 0 is 0" in
    assert(pathOffsetSize(0,0).path == "00/00/0000000000")
  "Path of position 1 is rounded down to 0" in
    assert(pathOffsetSize(1,0).path == "00/00/0000000000")
  "Path of position <fileSize - 1> is rounded down to 0" in
    assert(pathOffsetSize(fileSize - 1,0).path == "00/00/0000000000")
  "Path of position <fileSize> is folder 0 file <fileSize>" in
    assert(pathOffsetSize(fileSize,0).path == "00/00/0100000000")
  "Path of position <fileSize * 99> is folder 0 file <fileSize * 99>" in
    assert(pathOffsetSize(fileSize * 99L,0).path == "00/00/9900000000")
  "Path of position <fileSize * 100 + 1> is folder 1 file <fileSize * 100>" in
    assert(pathOffsetSize(fileSize * 100L + 1,0).path == "00/01/10000000000")
  "Path of position <fileSize * 10000> is folder 01/00 file <fileSize * 10000>" in
    assert(pathOffsetSize(fileSize * 10000L,0).path == "01/00/1000000000000")
  "Path of very large position (MaxLong/2) is represented correctly" in
    // MaxLong/2 = 4611686018427387903
    // Expected:   4611686018400000000 as file name, rounded down
    // Expected:          01           as middle folder name
    // Expected:   4611686             as leading folder name
    assert(pathOffsetSize(Long.MaxValue / 2,0).path == "4611686/01/4611686018400000000")
  "Position near MaxLong, i.e. 9e18 and beyond, yields an exception" in
    intercept[IllegalArgumentException](pathOffsetSize(9000000000000000000L,0))

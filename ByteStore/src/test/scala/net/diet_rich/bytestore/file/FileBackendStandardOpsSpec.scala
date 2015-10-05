package net.diet_rich.bytestore.file

import org.specs2.Specification

import net.diet_rich.common.test.bytes

class FileBackendStandardOpsSpec extends Specification with Common {
  def is = sequential ^ s2"""
General tests for the byte store file backend, starting with an empty store of block size 4
 ${eg{ read(0, 1) === List(bytes(0)) }}
 ${eg{ write(bytes(1, 2), 3); success }}
 ${eg{ read(2, 6) === List(bytes(0, 1), bytes(2, 0)) }}
 ${eg{ readRaw(3, 9) === List(Right(bytes(1)), Right(bytes(2)), Left(3), Left(1)) }}
  """
}

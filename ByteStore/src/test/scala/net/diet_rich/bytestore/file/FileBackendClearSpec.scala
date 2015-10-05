package net.diet_rich.bytestore.file

import org.specs2.Specification

import net.diet_rich.common.test.bytes

class FileBackendClearSpec extends Specification with Common {
  def is = sequential ^ s2"""
Tests for the clear method of the byte store file backend, starting with an empty store of block size 4
 ${eg{ write(bytes(1, 2, 3), 3); success }}
 ${eg{ clear(4, 5); success }}
 ${eg{ readRaw(0, 12) === List(Right(bytes(0, 0, 0, 1)), Right(bytes(0, 3)), Left(2), Left(4)) }}
 ${eg{ clear(5, 6); success }}
 ${eg{ readRaw(0, 12) === List(Right(bytes(0, 0, 0, 1)), Right(bytes(0)), Left(3), Left(4)) }}
 ${eg{ clear(3, 5); success }}
 ${eg{ readRaw(0, 12) === List(Right(bytes(0, 0, 0)), Left(1), Left(4), Left(4)) }}
  """
}

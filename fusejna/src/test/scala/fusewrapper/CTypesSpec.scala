package fusewrapper

import fusewrapper.FuseTestWrapper.INSTANCE.fuseWrapperTest_checkForCTypes
import fusewrapper.struct.CTypesReference
import org.specs2.Specification

class CTypesSpec extends Specification { def is = s2"""
    For the ctypes test structure, it is checked that
      the structure has the correct size $correctSize
      the fields are in the correct order $orderOfFields
  """

  lazy val (actualSize, ctypes) = {
    val ctypes = new CTypesReference()
    val actualSize = fuseWrapperTest_checkForCTypes(ctypes.size(), ctypes)
    (actualSize, ctypes)
  }

  def correctSize = actualSize === ctypes.size()

  def orderOfFields = {
    (ctypes.dev_t.intValue() === 1) and
      (ctypes.gid_t.intValue() === 2) and
      (ctypes.ino_t.intValue() === 3) and
      (ctypes.mode_t.intValue() === 4) and
      (ctypes.nlink_t.intValue() === 5) and
      (ctypes.off_t.intValue() === 6) and
      (ctypes.size_t.intValue() === 7) and
      (ctypes.uid_t.intValue() === 8) and
      (ctypes.unsigned.intValue() === 9) and
      (ctypes.endOfStructure.intValue() === 10)
  }
}

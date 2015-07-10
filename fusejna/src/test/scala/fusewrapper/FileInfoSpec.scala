package fusewrapper

import org.specs2.Specification

import fusewrapper.FuseTestWrapper.INSTANCE.fuseWrapperTest_checkForFuseFileInfo
import fusewrapper.struct.FuseFileInfoReference

class FileInfoSpec extends Specification { def is = s2"""
    For the file info structure, it is checked that
      the structure has the correct size $correctSize
      the fields are in the correct order $orderOfFields
  """

  lazy val (actualSize, info) = {
    val info = new FuseFileInfoReference()
    val actualSize = fuseWrapperTest_checkForFuseFileInfo(info.size(), info)
    (actualSize, info)
  }

  def correctSize = actualSize === info.size()

  def orderOfFields = {
    (info.flags.intValue() === 1) and
      (info.fh_old.intValue() === 2) and
      (info.writepage.intValue() === 3) and
      (info.fh.intValue() === 4) and
      (info.lock_owner.intValue() === 5) and
      (info.more_flags.intValue() === -1)
  }
}

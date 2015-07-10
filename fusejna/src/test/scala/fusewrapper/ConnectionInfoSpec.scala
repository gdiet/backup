package fusewrapper

import org.specs2.Specification

import fusewrapper.FuseTestWrapper.INSTANCE.fuseWrapperTest_checkForFuseConnectionInfo
import fusewrapper.ctype.Unsigned
import fusewrapper.struct.FuseConnectionInfoReference

class ConnectionInfoSpec extends Specification { def is = s2"""
    For the connection info structure, it is checked that
      the structure has the correct size $correctSize
      the fields are in the correct order $orderOfFields
  """

  lazy val (actualSize, info) = {
    val info = new FuseConnectionInfoReference()
    val actualSize = fuseWrapperTest_checkForFuseConnectionInfo(info.size(), info)
    (actualSize, info)
  }

  def correctSize = actualSize === info.size()

  def orderOfFields = {
    (info.proto_major.intValue() === 1) and
      (info.proto_minor.intValue() === 2) and
      (info.async_read.intValue() === 3) and
      (info.max_write.intValue() === 4) and
      (info.max_readahead.intValue() === 5) and
      (info.capable.intValue() === 6) and
      (info.want.intValue() === 7) and
      (info.max_background.intValue() === 8) and
      (info.congestion_threshold.intValue() === 9) and
      (info.reserved.toSeq must contain(beEqualTo(new Unsigned(-1))).forall)
  }
}

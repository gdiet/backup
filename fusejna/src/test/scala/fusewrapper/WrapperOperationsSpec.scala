package fusewrapper

import fusewrapper.struct.FuseWrapperOperations
import org.specs2.Specification

import FuseTestWrapper.INSTANCE.fuseWrapperTest_sizeOfFuseOperations

class WrapperOperationsSpec extends Specification { def is = s2"""
    The operations structure should have the correct size $correctSize
  """

  def correctSize = fuseWrapperTest_sizeOfFuseOperations() === new FuseWrapperOperations().size()
}

package fusewrapper

import org.specs2.Specification

import fusewrapper.FuseTestWrapper.INSTANCE.fuseWrapperTest_checkForStat
import fusewrapper.struct.StatReference

class StatSpec extends Specification { def is = s2"""
    For the stat structure, it is checked that
      the structure has the correct size $correctSize
      the fields are in the correct order $orderOfFields
  """

  lazy val (actualSize, stat) = {
    val stat = new StatReference()
    val actualSize = fuseWrapperTest_checkForStat(stat.size(), stat)
    (actualSize, stat)
  }

  def correctSize = actualSize === stat.size()

  def orderOfFields = {
    (stat.st_dev.intValue() === 1) and
      (stat.st_ino.intValue() === 2) and
      (stat.st_nlink.intValue() === 3) and
      (stat.st_mode.intValue() === 4) and
      (stat.st_uid.intValue() === 5) and
      (stat.st_gid.intValue() === 6) and
      (stat.st_rdev.intValue() === 7) and
      (stat.st_size.intValue() === 8) and
      (stat.diverse_1 === -1) and
      (stat.diverse_2.toSeq must contain(beEqualTo(-1)).forall)
  }
}

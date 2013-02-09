// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.backup

import net.diet_rich.dedup.database._
import net.diet_rich.util.io._

trait MatchCheck {
  def processMatchingTimeAndSize(reader: => SeekReader, referenceData: FullDataInformation): Option[(Print, SeekReader)]
}

class PrintMatchCheck(calculatePrint: SeekReader => Print) extends MatchCheck {
  def processMatchingTimeAndSize(reader: => SeekReader, referenceData: FullDataInformation): Option[(Print, SeekReader)] =
    calculatePrint(reader) match {
      case referenceData.print => reader.close; None
      case print => Some(print, reader)
    }
}

class NoPrintMatch extends MatchCheck {
  def processMatchingTimeAndSize(reader: => SeekReader, referenceData: FullDataInformation): Option[(Print, SeekReader)] =
    None
}

class IgnoreMatch(calculatePrint: SeekReader => Print) extends MatchCheck {
  def processMatchingTimeAndSize(reader: => SeekReader, referenceData: FullDataInformation): Option[(Print, SeekReader)] =
    Some(calculatePrint(reader), reader)
}

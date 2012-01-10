// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup

import net.diet_rich.util.data.Digester

class FSSettings {
  val hashProvider: HashProvider = new HashProvider {
    override def getHashDigester = Digester.hash("MD5")
  }
  val printCalculator: PrintCalculator =
    PrintCalculator("start:crcadler", 8192)
}
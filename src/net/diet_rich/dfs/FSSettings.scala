// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dfs

import net.diet_rich.util.data.Digester

class FSSettings(val hashProvider: HashProvider, val printCalculator: PrintCalculator)

object FSSettings {
  def apply(hashAlgorithm: String, printAlgorithm: String, printLength: Int) : FSSettings = new FSSettings(
    HashProvider(hashAlgorithm),
    PrintCalculator(printAlgorithm, printLength)
  )
  
  def default: FSSettings = FSSettings("MD5", "start:crcadler", 8192)
}
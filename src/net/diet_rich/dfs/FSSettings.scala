// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dfs

class FSSettings(val hashProvider: HashAndPrintProvider, val printLength: Int)

object FSSettings {
  def default: FSSettings = new FSSettings(HashAndPrintProvider("MD5"), 8192)
}
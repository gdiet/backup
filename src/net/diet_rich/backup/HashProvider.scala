// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup

import net.diet_rich.util.data.Digester.BytesDigester

trait HashProvider {
  def getHashDigester : BytesDigester
}
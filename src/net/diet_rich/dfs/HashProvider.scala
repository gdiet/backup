// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dfs

import net.diet_rich.util.data.Digester
import net.diet_rich.util.data.Digester.BytesDigester

trait HashProvider {
  def getHashDigester : BytesDigester
  def name : String
}

object HashProvider {
  def apply(hashAlgorithm: String) : HashProvider = new HashProvider {
    override def getHashDigester = Digester.hash(hashAlgorithm)
    override def name = hashAlgorithm
    override def toString = hashAlgorithm + " provider"
  }
}
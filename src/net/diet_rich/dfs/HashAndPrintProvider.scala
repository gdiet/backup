// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dfs

import net.diet_rich.util.BytesDigester
import net.diet_rich.util.PrintDigester

trait HashAndPrintProvider {
  def getHashDigester : BytesDigester
  def getPrintDigester : PrintDigester
  def name : String
}

object HashAndPrintProvider {
  def apply(hashAlgorithm: String) : HashAndPrintProvider = new HashAndPrintProvider {
    override def getHashDigester = BytesDigester(hashAlgorithm)
    override def getPrintDigester = PrintDigester.crcAdler
    override def name = hashAlgorithm
    override def toString = hashAlgorithm + " hash with crcAdler print"
  }
}
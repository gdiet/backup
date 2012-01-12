// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dfs

import net.diet_rich.util.data.Digester
import net.diet_rich.util.io.OutputStream
import net.diet_rich.util.io.RandomAccessInput
import DataDefinitions._

trait PrintCalculator {
  /** Calculates the finger print, first resetting the input. */
  def calculate(input: RandomAccessInput) : Long
  /** Calculates (finger print, length) while copying the input after resetting it. */
  def calculate(input: RandomAccessInput, output: OutputStream) : SizePrint
  
  private def reset(input: RandomAccessInput) : RandomAccessInput = {
    input seek 0
    input
  }
}

object PrintCalculator {
  def apply(name: String, length: Int) : PrintCalculator =
    name match {
      case "start:crcadler" => startCrcAdler(length)
    }
  private def startCrcAdler(length: Int) : PrintCalculator = new PrintCalculator {
    override def calculate(input: RandomAccessInput) : Long =
      Digester.crcadler().writeAnd (reset(input) readFully length) getDigest
    override def calculate(input: RandomAccessInput, output: OutputStream) = {
      val start = reset(input) readFully length
      val print = Digester.crcadler().writeAnd(start).getDigest
      output write start
      val read = input copyTo output
      SizePrint(print, start.length + read)
    }
  }
}

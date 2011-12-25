// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.fs
import net.diet_rich.util.io.OutputStream

case class TimeSize(time: Long, size: Long)
case class TimeSizePrint(time: Long, size: Long, print: Long)
case class TimeSizePrintHash(time: Long, size: Long, print: Long, hash: Array[Byte])

trait DataAccess {
  def timeSize : TimeSize
  def timeSizePrint : TimeSizePrint
  def timeSizePrintHash : TimeSizePrintHash
  def writeData(out: OutputStream) : TimeSizePrintHash
}
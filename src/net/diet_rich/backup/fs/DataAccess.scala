// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.fs

import net.diet_rich.util.Bytes
import net.diet_rich.util.io.Closeable
import net.diet_rich.util.io.OutputStream
import net.diet_rich.util.io.RandomAccessFileInput
import net.diet_rich.util.io.RandomAccessInput
import java.io.File

case class TimeSize(time: Long, size: Long)
case class TimeSizePrint(time:  Long, size:  Long, print:  Long)
case class TimeSizePrintHash(time:  Long, size:  Long, print:  Long, hash: Bytes)

trait DataAccess extends Closeable {
  def timeSize : TimeSize
  def timeSizePrint : TimeSizePrint
  def timeSizePrintHash : TimeSizePrintHash
  def writeData(out: OutputStream) : TimeSizePrintHash
}

object DataAcess {
  def read(config: FSConfig, file: File) : DataAccess = new DataAccess {
    var input_ : Option[RandomAccessInput] = None
    def input : RandomAccessInput = {
      val result = input_.getOrElse{
        input_ = Some(new RandomAccessFileInput(file))
        input_ get
      }
      result.seek(0)
      result
    }
    override def timeSize : TimeSize =
      TimeSize(file.lastModified, file.length)
    override def timeSizePrint : TimeSizePrint =
      TimeSizePrint(file.lastModified, file.length, config.printCalculator.print(input))
    override def timeSizePrintHash : TimeSizePrintHash = throw new UnsupportedOperationException
    override def writeData(out: OutputStream) : TimeSizePrintHash = throw new UnsupportedOperationException
    override def close : Unit = input_ foreach (_.close)
  }
}
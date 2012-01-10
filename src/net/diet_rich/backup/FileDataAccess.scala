// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup

import java.io.Closeable
import net.diet_rich.util.Bytes
import net.diet_rich.util.data.Digester.BytesDigester
import net.diet_rich.util.data.Digester.ChecksumDigester
import net.diet_rich.util.io.OutputStream
import net.diet_rich.util.io.RandomAccessFileInput
import net.diet_rich.util.io.RandomAccessInput
import java.io.File

/** Multiple calls to the methods may not return the same results since
 *  the file in the file system may have changed between two calls.
 */
trait FileDataAccess extends Closeable {
  import FileDataAccess._
  def timeSize : TimeSize
  def timeSizePrint : TimeSizePrint
  def timeSizePrintHash : TimeSizePrintHash
  def writeData(out: OutputStream) : TimeSizePrintHash
}

object FileDataAccess {
  
  def apply(file: File, printCalculator: PrintCalculator, hashProvider: HashProvider): FileDataAccess = new FileDataAccess {
    private var _input: Option[RandomAccessInput] = None
    private def input: RandomAccessInput = _input getOrElse {
      _input = Some(new RandomAccessFileInput(file)); input
    }
    
    override def timeSize : TimeSize =
      TimeSize(file lastModified, file length)

    override def timeSizePrint : TimeSizePrint =
      TimeSizePrint(file lastModified, file length, printCalculator calculate input)
    
    override def timeSizePrintHash : TimeSizePrintHash = {
      val hash = hashProvider getHashDigester
      val info = printCalculator calculate (input, hash)
      TimeSizePrintHash(file lastModified, info size, info print, hash getDigest)
    }
    
    override def writeData(out: OutputStream) : TimeSizePrintHash = {
      val hash = hashProvider getHashDigester
      val info = printCalculator calculate (input, OutputStream tee (hash, out))
      TimeSizePrintHash(file lastModified, info size, info print, hash getDigest)
    }
    
    override def close : Unit = _input foreach (_.close)
  }
}
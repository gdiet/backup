// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.datastore

import java.io.ByteArrayOutputStream
import java.util.zip._
import net.diet_rich.dedup.database.Method
import net.diet_rich.util.io._

object StoreMethods {
  def wrapStore(source: ByteSource, method: Method): ByteSource = method match {
    case Method.STORE => source
    case Method.DEFLATE => 
      val deflater = new Deflater(Deflater.BEST_COMPRESSION, true)
      new DeflaterInputStream(sourceAsInputStream(source), deflater)
  }
  
  def wrapRestore(source: ByteSource, method: Method): ByteSource = method match {
    case Method.STORE => source
    case Method.DEFLATE => 
      val inflater = new Inflater(true)
      // appendByte is needed as fix for a bug in Inflater, see e.g.
      // 6519463 : Unexpected end of ZLIB when using GZIP on some files
      new InflaterInputStream(sourceAsInputStream(appendByte(source, 0)), inflater)
  }
}
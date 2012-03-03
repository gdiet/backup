// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dfs.datastore

import net.diet_rich.util.data.Bytes
import net.diet_rich.util.io.InputStream
import net.diet_rich.util.io.OutputStream

trait DataStore {
  import DataStore._

  def dataSink: StoreOutput
  def dataSource(parts: Seq[StoredPart]): InputStream
  
}

object DataStore {
  case class StoredPart(start: Long, end: Long)
  
  trait StoreOutput {
    def out: OutputStream
    def result: Seq[StoredPart]
  }
  
  def memoryStore(datasize: Int): DataStore = new DataStore {
    val data = new Array[Byte] (datasize)
    def dataSink: StoreOutput = {
      throw new UnsupportedOperationException
    }
    def dataSource(parts: Seq[StoredPart]): InputStream = {
      new InputStream {
        val partIterator = parts.iterator
        def nextPart = { if (partIterator.hasNext) Some(partIterator.next) else None }
        var currentPart = nextPart
        var currentIndex = nextPart.map(_.start)
        def read(bytes: Bytes) : Bytes = {
          if (currentPart.isEmpty || bytes.length == 0)
            bytes.keepFirst(0)
          else {
            val len = math.min(bytes.length, currentPart.get.end - currentIndex.get)
            if (len == 0) {
              currentPart = nextPart
              currentIndex = nextPart.map(_.start)
              read(bytes)
            } else {
//              System.ar
              Bytes(0)
            }
          }
        }
      }
      // FIXME continue
      throw new UnsupportedOperationException
    }
  }
}
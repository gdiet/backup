package net.diet_rich.dedup.core

import java.io.OutputStream

import scala.util.Random

import org.specs2.Specification

import net.diet_rich.dedup.core.data.StoreMethod
import net.diet_rich.dedup.core.data.file.FileBackend
import net.diet_rich.dedup.core.meta.{FreeRanges, MetaUtils}
import net.diet_rich.dedup.util.{Memory, init}

class StoreOutputStreamSpec extends Specification with MetaUtils { def is = s2"""
${"Tests for the store output stream".title}

The store output stream should store an empty file $storeEmptyFile
The store output stream should store a small file $storeSmallFile
The store output stream should store a medium sized file $storeMediumFile
The store output stream should store a large file $storeLargeFile
"""

  def store(dataHandler: OutputStream => Unit) = withEmptyMetaBackend { metaBackend =>
    val freeRanges = new FreeRanges(Seq((0L, Long.MaxValue)), FileBackend.nextBlockStart(_, 1000L))
    val storeLogic = new StoreLogic(metaBackend, (_,_) => Unit, freeRanges, "MD5", StoreMethod.STORE, 4)
    val out = new StoreOutputStream(storeLogic, _ => Unit)
    dataHandler(out)
    out.close()
    success
  }

  val data = init(new Array[Byte](4000))(Random nextBytes)

  def storeEmptyFile = store(_ => Unit)
  def storeSmallFile = store(_.write(1))
  def storeMediumFile = store { out =>
    for (i <- 1 to 50) out.write(i)
    for (i <- 1 to 50) out.write(data)
  }
  def storeLargeFile =
    Memory.reserved(Memory.available - 400000) { case _ =>
      store { out =>
        for (i <- 1 to 50) out.write(i)
        for (i <- 1 to 200) out.write(data)
      }
    }
}

package net.diet_rich.dedup.ftpserver

import java.io.{File, RandomAccessFile, OutputStream}

import net.diet_rich.dedup.core.Source
import net.diet_rich.dedup.util._
import net.diet_rich.dedup.util.io.ExtendedByteArrayOutputStream

class CachingOutputStream(maxBytesToCache: Int, closeAction: Source => Unit) extends OutputStream {
  import CachingOutputStream._

  var sink: Either[ExtendedByteArrayOutputStream, RandomAccessFile] = Left(new ExtendedByteArrayOutputStream())
  var tempFile: Option[File] = None
  var open = true
  var writeOK = true
  override def write(i: Int): Unit = write(Array(i.toByte), 0, 1)
  override def write(data: Array[Byte]): Unit = write(data, 0, data.length)
  override def write(data: Array[Byte], offset: Int, length: Int): Unit = try {
    assert(writeOK)
    sink.fold({ out =>
      if (out.size > maxBytesToCache) switchToTempFile(data, offset, length) else {
        Memory.reserve(length * memoryConsumptionFactor) match {
          case _: Memory.Reserved => out.write(data, offset, length)
          case _: Memory.NotAvailable => switchToTempFile(data, offset, length)
        }
      }
    }, _.write(data, offset, length))
  } catch { case e: Throwable => writeOK = false; throw e }
  def switchToTempFile(data: Array[Byte], offset: Int, length: Int): Unit = sink match {
    case Right(_) => throw new IllegalStateException
    case Left(out) =>
      val file = init(File createTempFile ("ftpserver_", ".tmp"))(_ deleteOnExit())
      val access = new RandomAccessFile(file, "rw")
      tempFile = Some(file)
      sink = Right(access)
      try {
        access write (out.data, 0, out.size)
        access write (data, offset, length)
      } finally Memory.free(out.size * memoryConsumptionFactor)
  }
  override def close(): Unit = if (open) {
    open = false
    try {
      if (writeOK) closeAction(sink.fold(
        out => Source from (out.data, 0, out.size),
        out => init(Source from out)(_ reset)
      ))
    } finally {
      sink.fold(
        out => Memory.free(out.size * memoryConsumptionFactor),
        out => {out.close(); tempFile foreach (_ delete())}
      )
    }
  }
}

object CachingOutputStream {
  private val memoryConsumptionFactor = 4 // determined experimentally
}

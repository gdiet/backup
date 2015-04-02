package net.diet_rich.dedup.util.io

import java.io.ByteArrayOutputStream

// FIXME remove
class ExtendedByteArrayOutputStream extends ByteArrayOutputStream {
  def data = buf
}

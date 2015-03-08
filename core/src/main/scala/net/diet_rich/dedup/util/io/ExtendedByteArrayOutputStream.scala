package net.diet_rich.dedup.util.io

import java.io.ByteArrayOutputStream

class ExtendedByteArrayOutputStream extends ByteArrayOutputStream {
  def data = buf
}

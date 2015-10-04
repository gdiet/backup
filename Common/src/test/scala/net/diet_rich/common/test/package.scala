package net.diet_rich.common

import java.io.File

package object test {
  def delete(dir: File): Unit = {
    if (dir.isDirectory) dir.listFiles() foreach delete
    dir.delete()
  }
  def bytes(b: Byte*) = Bytes(Array(b:_*), 0, b.size)
}

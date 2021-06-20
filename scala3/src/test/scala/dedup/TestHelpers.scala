package dedup

import java.io.File
import scala.reflect._

def °[T: ClassTag]: String = classTag[T].runtimeClass.getTypeName

def delete(file: File): Unit =
  if (file.isDirectory) then file.listFiles.foreach(delete)
  file.delete()

trait TestDir:
  lazy val testDir: File =
    File(sys.props("java.io.tmpdir") + s"/dedupfs-test/${getClass.getTypeName}").tap(delete)

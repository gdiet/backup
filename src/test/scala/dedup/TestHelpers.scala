package dedup

import java.io.File
import scala.reflect.*

def °[T: ClassTag]: String = classTag[T].runtimeClass.getTypeName

def delete(file: File): Unit =
  if file.exists then
    if file.isDirectory then file.listFiles.foreach(delete)
    file.delete()

def !!! : Nothing = throw new Exception("Should not be reached in test.")

def suppressing[T](marker: String)(f: => T): T =
  try { sys.props.put(s"suppress.$marker", "true"); f }
  finally sys.props.remove(s"suppress.$marker")

trait TestFile:
  lazy val testFile: File =
    File(sys.props("java.io.tmpdir") + s"/dedupfs-test/${getClass.getTypeName}")
      .tap(delete)
      .tap(_.getParentFile.mkdirs)

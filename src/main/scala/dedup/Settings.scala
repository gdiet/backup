package dedup

import java.io.File
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicBoolean

case class Settings(
 repo: File,
 dbDir: File,
 temp: File,
 readonly: Boolean,
 copyWhenMoving: AtomicBoolean
) {
  val tempPath: Path = temp.toPath
  val dataDir = new File(repo, "data")
}
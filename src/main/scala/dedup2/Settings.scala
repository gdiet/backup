package dedup2

import java.io.File
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicBoolean

case class Settings(
 repo: File,
 temp: File,
 readonly: Boolean,
 copyWhenMoving: AtomicBoolean = new AtomicBoolean(false)
) {
  val tempPath: Path = temp.toPath
  val dataDir = new File(repo, "data")
}

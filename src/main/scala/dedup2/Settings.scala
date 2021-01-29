package dedup2

import java.io.File
import java.util.concurrent.atomic.AtomicBoolean

case class Settings(
 repo: File,
 readonly: Boolean,
 copyWhenMoving: AtomicBoolean = new AtomicBoolean(false)
) {
  val dataDir = new File(repo, "data")
}

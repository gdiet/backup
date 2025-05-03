package dedup
package server

import java.io.File
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicBoolean

case class Settings( repo          : File,
                     dbDir         : File,
                     temp          : File,
                     readOnly      : Boolean,
                     showDeleted   : AtomicBoolean,
                     copyWhenMoving: AtomicBoolean ):

  val tempPath: Path = temp.toPath
  val dataDir : File = store.dataDir(repo)

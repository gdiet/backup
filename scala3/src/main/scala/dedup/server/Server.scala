package dedup
package server

import jnr.ffi.Platform.OS.WINDOWS
import jnr.ffi.{Platform, Pointer}
import ru.serce.jnrfuse.struct.{FileStat, FuseFileInfo, Statvfs, Timespec}
import ru.serce.jnrfuse.{FuseFillDir, FuseStubFS}

class Server(settings: Settings) extends FuseStubFS with util.ClassLogging {

}

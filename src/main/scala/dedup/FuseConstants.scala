package dedup

import ru.serce.jnrfuse.ErrorCodes

trait FuseConstants {
  val EEXIST    : Int = -ErrorCodes.EEXIST
  val EIO       : Int = -ErrorCodes.EIO
  val EISDIR    : Int = -ErrorCodes.EISDIR
  val ENOENT    : Int = -ErrorCodes.ENOENT
  val ENOTDIR   : Int = -ErrorCodes.ENOTDIR
  val EOVERFLOW : Int = -ErrorCodes.EOVERFLOW
  val EROFS     : Int = -ErrorCodes.EROFS
  val OK        : Int = 0
}

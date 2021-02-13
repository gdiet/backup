package dedup

import ru.serce.jnrfuse.ErrorCodes

trait FuseConstants {
  val EEXIST    : Int = -ErrorCodes.EEXIST
  val EIO       : Int = -ErrorCodes.EIO
  val EINVAL    : Int = -ErrorCodes.EINVAL
  val EISDIR    : Int = -ErrorCodes.EISDIR
  val ENOENT    : Int = -ErrorCodes.ENOENT
  val ENOTDIR   : Int = -ErrorCodes.ENOTDIR
  val ENOTEMPTY : Int = -ErrorCodes.ENOTEMPTY
  val EOVERFLOW : Int = -ErrorCodes.EOVERFLOW
  val EROFS     : Int = -ErrorCodes.EROFS
  val OK        : Int = 0
}

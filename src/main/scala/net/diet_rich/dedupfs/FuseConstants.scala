package net.diet_rich.dedupfs

import ru.serce.jnrfuse.ErrorCodes

object FuseConstants {
  val O777     : Int = 511 // octal 0777
  val OK       : Int = 0
  val EEXIST   : Int = -ErrorCodes.EEXIST
  val EIO      : Int = -ErrorCodes.EIO
  val EISDIR   : Int = -ErrorCodes.EISDIR
  val ENOENT   : Int = -ErrorCodes.ENOENT
  val ENOTDIR  : Int = -ErrorCodes.ENOTDIR
  val ENOTEMPTY: Int = -ErrorCodes.ENOTEMPTY
}


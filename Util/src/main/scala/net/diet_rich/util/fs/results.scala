package net.diet_rich.util.fs

sealed trait RenameResult
case object RenameOk extends RenameResult
case object TargetExists extends RenameResult
case object TargetParentDoesNotExist extends RenameResult
case object TargetParentNotADirectory extends RenameResult

sealed trait DeleteDirResult
case object DeleteDirOk extends DeleteDirResult
case object DirNotEmpty extends DeleteDirResult
case object DirNotFound extends DeleteDirResult

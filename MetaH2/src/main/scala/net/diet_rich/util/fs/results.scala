package net.diet_rich.util.fs

sealed trait MkdirResult
case object MkdirOk extends MkdirResult
case object MkdirParentNotFound extends MkdirResult
case object MkdirParentNotADir extends MkdirResult
case object MkdirExists extends MkdirResult
case object MkdirIllegalPath extends MkdirResult

sealed trait RenameResult
case object RenameOk extends RenameResult
case object RenameTargetExists extends RenameResult
case object RenameParentDoesNotExist extends RenameResult
case object RenameParentNotADirectory extends RenameResult
case object RenameIllegalPath extends RenameResult

sealed trait DeleteResult
case object DeleteOk extends DeleteResult
case object DeleteHasChildren extends DeleteResult
case object DeleteNotFound extends DeleteResult

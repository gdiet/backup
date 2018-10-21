package net.diet_rich.util.fs

sealed trait RenameResult
case object RenameOk extends RenameResult
case object TargetExists extends RenameResult
case object TargetParentDoesNotExist extends RenameResult
case object TargetParentNotADirectory extends RenameResult

sealed trait DeleteResult
case object DeleteOk extends DeleteResult
case object DeleteHasChildren extends DeleteResult
case object DeleteNotFound extends DeleteResult

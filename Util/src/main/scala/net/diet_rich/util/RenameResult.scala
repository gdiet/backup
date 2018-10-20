package net.diet_rich.util

sealed trait RenameResult
case object RenameOk extends RenameResult
case object TargetExists extends RenameResult
case object TargetParentDoesNotExist extends RenameResult
case object TargetParentNotADirectory extends RenameResult

package dedup._manual

object Restore extends App:
  val repo = java.io.File("./manual")
  val restore = java.io.File(repo, "restore")
  dedup.delete(restore)
  restore.mkdir()
  dedup.fsc(s"repo=$repo", "restore",
    "/backup",
    "manual/restore"
  )

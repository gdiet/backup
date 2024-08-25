package dedup._manual

object BackupTool extends App:
  val repo = java.io.File("./manual")
  dedup.fsc("repo=./manual", "dbBackup=false", "backup",
    """src""",
    "/?backup/[yyyy]/![yyyy.MM.dd_HH.mm_ss]",
    "reference=/backup/*/*"
  )

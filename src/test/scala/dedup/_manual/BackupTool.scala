package dedup._manual

object BackupTool extends App:
  val repo = java.io.File("./manual")
  dedup.fsc("repo=./manual", "dbBackup=false", "backup",
    """c:\georg\temp\dedupfs-demo\Beispieldaten""",
    "/backup/?[yyyy]/![yyyy.MM.dd_HH.mm_ss]"
//    , "reference=/target/backup/*/*"
  )

package dedup._manual

object BackupTool extends App:
  val repo = java.io.File("./manual")
  dedup.fsc("repo=./manual", "dbBackup=false", "backup",
    """c:\Users\Georg.Dietrich\Downloads\decryption\target\*""",
    "/target/backup/?[yyyy]/![yyyy.MM.dd_HH.mm_ss]",
    "reference=/tar?et/backup/*/*"
  )

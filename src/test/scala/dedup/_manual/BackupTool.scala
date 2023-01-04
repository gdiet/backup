package dedup._manual

object BackupTool extends App:
  val repo = java.io.File("./manual")
  dedup.fsc("repo=./manual", "backup", 
    """c:\Users\Georg.Dietrich\Downloads\decryption\target\*""",
    "/backup/![yyyy]/[yyyy.MM.dd_HH.mm]/",
    "reference=/target"
  )

package dedup._manual

object BackupTool extends App:
  val repo = java.io.File("./manual")
  dedup.fsc("repo=./manual", "backup", 
    """c:\Users\Georg.Dietrich\Downloads\decryption\target\*""",
    "/[yyyy.MM.dd_HH.mm_ss]/",
    "reference=/target"
  )

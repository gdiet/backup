package dedup._manual

object Find extends App:
  val repo = java.io.File("./manual")
  dedup.fsc("repo=./manual", "dbBackup=false", "find",
    "jr?/*/j?va*.exe"
  )

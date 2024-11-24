package dedup._manual

object Find extends App:
  dedup.fsc("repo=./manual", "dbBackup=false", "find", "jr?/*/j?va*.exe")

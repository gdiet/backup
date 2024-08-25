package dedup._manual

object Migrate1 extends App:
  val repo = java.io.File("./manual")
  dedup.fsc("repo=./manual", "db-migrate1")

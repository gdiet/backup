package dedup._manual

object Migrate2 extends App:
  val repo = java.io.File("./manual")
  dedup.fsc("repo=./manual", "db-migrate2")

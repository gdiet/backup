package dedup._manual

object CleanInit extends App:
  val repo = java.io.File("./manual")
  dedup.delete(repo)
  require(repo.mkdir())
  dedup.init(("repo", "./manual"))

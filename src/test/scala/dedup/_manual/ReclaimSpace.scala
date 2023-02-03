package dedup._manual

object ReclaimSpace extends App:
  val repo = java.io.File("./manual")
  dedup.reclaimSpace(("repo", "./manual"))

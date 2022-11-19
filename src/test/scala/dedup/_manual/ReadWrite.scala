package dedup._manual

object ReadWrite extends App:
  val repo = java.io.File("./manual")
  dedup.mount(("repo", "./manual"), ("mount", "L:\\"))

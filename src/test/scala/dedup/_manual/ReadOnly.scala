package dedup._manual

object ReadOnly extends App:
  val repo = java.io.File("./manual")
  dedup.mount(("repo", "./manual"), ("readOnly", "true"), ("mount", "L:\\"))

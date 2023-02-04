package dedup._manual

object ReadOnly extends App:
  dedup.mount(("repo", "./manual"), ("readOnly", "true"), ("mount", "L:\\"))

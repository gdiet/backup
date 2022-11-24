package dedup._manual

//object ReadOnly2 extends App:
//  val repo = java.io.File("./manual")
//  dedup.mount2(("repo", "./manual"), ("readOnly", "true"), ("mount", "L:\\"))
//
object ReadWrite2 extends App:
  val repo = java.io.File("./manual")
  dedup.mount2(("repo", "./manual"), ("mount", "L:\\"))

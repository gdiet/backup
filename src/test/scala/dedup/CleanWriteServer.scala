package dedup

object CleanWriteServer extends App {
  CleanInit.main(args)
  Server.main(Array("write"))
}

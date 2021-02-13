package dedup

object CleanWriteServer extends App {
  CleanInit.main(args)
  Main.main(Array("write"))
}

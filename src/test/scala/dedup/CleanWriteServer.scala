package dedup

object CleanWriteServer extends App {
  sys.props.update("LOG_BASE", "./")
  CleanInit.main(args)
  Main.main(Array("write", "mount=/home/georg/temp/mnt"))
}

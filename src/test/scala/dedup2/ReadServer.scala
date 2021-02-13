package dedup2

object ReadServer extends App {
  sys.props.update("LOG_BASE", "./")
  Main.main(Array())
}

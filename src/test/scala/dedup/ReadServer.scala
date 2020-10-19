package dedup

object ReadServer extends App {
  sys.props.update("LOG_BASE", "./")
  Server.main(Array())
}

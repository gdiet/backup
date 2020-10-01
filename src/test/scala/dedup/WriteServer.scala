package dedup

object WriteServer extends App {
  sys.props.update("LOG_BASE", "./")
  Server.main(Array("write"))
}

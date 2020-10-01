package dedup

object CheckServer extends App {
  sys.props.update("LOG_BASE", "./")
  Server.main(Array("check"))
}

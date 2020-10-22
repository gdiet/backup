package dedup

object Blacklist extends App {
  sys.props.update("LOG_BASE", "./")
  Server.main(Array("blacklist"))
}

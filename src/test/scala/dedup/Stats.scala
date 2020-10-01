package dedup

object Stats extends App {
  sys.props.update("LOG_BASE", "./")
  Server.main(Array("stats"))
}

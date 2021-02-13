package dedup

object WriteServer extends App {
  sys.props.update("LOG_BASE", "./")
  Main.main(Array("write"))
}

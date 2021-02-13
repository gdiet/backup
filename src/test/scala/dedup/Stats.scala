package dedup

object Stats extends App {
  sys.props.update("LOG_BASE", "./")
  Main.main(Array("stats"))
}

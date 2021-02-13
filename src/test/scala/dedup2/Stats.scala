package dedup2

object Stats extends App {
  sys.props.update("LOG_BASE", "./")
  Main.main(Array("stats"))
}

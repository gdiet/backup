package dedup

object WriteServerGui extends App {
  sys.props.update("LOG_BASE", "./")
  ServerGui.main(Array("write"))
}

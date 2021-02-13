package dedup2

object Reclaim2 extends App {
  sys.props.update("LOG_BASE", "./")
  Main.main(Array("reclaimspace2"))
}

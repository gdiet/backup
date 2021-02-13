package dedup

object Reclaim2 extends App {
  sys.props.update("LOG_BASE", "./")
  Main.main(Array("reclaimspace2"))
}

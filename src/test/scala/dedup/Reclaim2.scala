package dedup

object Reclaim2 extends App {
  sys.props.update("LOG_BASE", "./")
  Server.main(Array("reclaimspace2"))
}

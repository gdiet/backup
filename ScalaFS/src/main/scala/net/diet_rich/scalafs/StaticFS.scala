package net.diet_rich.scalafs

import net.diet_rich.util.ClassLogging

// FIXME remove
object StaticFS extends FileSystem with ClassLogging {
  override def getNode(path: String): Option[Node] = {
    log.info(s"getNode($path)")
    path match {
      case "/" => Some(Root)
      case "/hello" => Some(Hello)
      case _ => None
    }
  }
  override def splitParentPath(path: String): Option[(String, String)] = ???
}

case object Root extends Dir {
  override def list: Seq[Node] = Seq(Hello)
  override def name: String = ""
  override def mkDir(child: String): Boolean = false
}

case object Hello extends Dir {
  override def list: Seq[Node] = Seq()
  override def name: String = "hello"
  override def mkDir(child: String): Boolean = false
}

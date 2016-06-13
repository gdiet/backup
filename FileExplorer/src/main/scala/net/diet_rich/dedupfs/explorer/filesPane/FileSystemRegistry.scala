package net.diet_rich.dedupfs.explorer.filesPane

class FileSystemRegistry {
  import FileSystemRegistry._

  // TODO make immutable
  private var fileSystems = Map[Scheme, Path => Option[FilesPaneDirectory]]()

  def withScheme(scheme: Scheme, factory: Path => Option[FilesPaneDirectory]): FileSystemRegistry = synchronized {
    require(!fileSystems.contains(scheme), s"File system for scheme $scheme already registered")
    fileSystems += scheme -> factory
    this
  }

  def remove(scheme: Scheme): Unit = synchronized { fileSystems -= scheme }

  def get(url: String): Option[FilesPaneDirectory] = url.split("://", 2) match {
    case Array(scheme, path) => fileSystems get scheme flatMap (_(path))
    case _ => None
  }
}

object FileSystemRegistry {
  type Path = String
  type Scheme = String
}

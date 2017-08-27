package net.diet_rich.dedup.meta

import TreePath.RichPath

object MetaBackend {
  val pathInRepository = "meta"
}

/** File system tree and metadata methods. */
trait MetaBackend {
  /** Returns the tree entry or `None` if the entry does not exist or is marked deleted. */
  def entry(key: Long): Option[TreeEntry]

  /** Returns the tree entry's children. Returns multiple children with the same name if present. May return children
    * of entries marked deleted, if the children are not marked deleted.
    *
    * @return The tree entry's children. */
  def children(parent: Long): Iterable[TreeEntry]

  /** Returns the settings map for this metadata repository. */
  def settings: String Map String

  /** Returns child nodes by name. Returns multiple children with the same name if present. May return children
    * of entries marked deleted, if the children are not marked deleted.
    *
    * @return The tree entry's children named as requested. */
  def child(parent: Long, name: String): Iterable[TreeEntry] =
    children(parent) filter (_.name == name)

  /** @return The tree entries reachable by the path. */
  def entry(path: String): Iterable[TreeEntry] =
    entry(path.pathElements)
  /** @return The tree entries reachable by the path. */
  def entry(path: Array[String]): Iterable[TreeEntry] =
    path.foldLeft(Iterable(TreeEntry.root)) { (nodes, name) => nodes flatMap (node => child(node.key, name)) }

  /** @return The path for a tree entry or `None` if it does not exist. */
  def path(key: Long): Option[String] =
    if (key == TreeEntry.root.key) Some(TreePath.rootPath)
    else entry(key) flatMap {entry => path(entry.parent) map (_ + TreePath.pathSeparator + entry.name)}

  /** Returns the hash algorithm setting for this metadata repository. */
  def hashAlgorithm: String = settings(net.diet_rich.dedup.Settings.hashAlgorithmKey)
}

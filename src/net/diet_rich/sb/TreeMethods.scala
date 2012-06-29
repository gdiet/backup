package net.diet_rich.sb

import TreeMethods._
import TreeDB._

trait TreeMethods extends TreeDB {
  
  private def createFromParent(path: String) : Option[Long] =
    getOrCreate(parentPath(path)) flatMap { parent => create(parent, nameFromPath(path)) }
  
  /** Create any nodes elements in the path.
   * 
   *  @return ID, None if path already existed or node was missing and could not be created.
   *  If None is returned, parts of the path may already have been created. */
  def create(path: String) : Option[Long] =
    entry(path) match {
      case None => createFromParent(path)
      case _ => None
    }
  
  /** Create any missing nodes in the path.
   * 
   *  @return ID, None if node was missing and could not be created.
   *  If None is returned, parts of the path may already have been created. */
  def getOrCreate(path: String) : Option[Long] =
    entry(path) map(_ id) orElse createFromParent(path)

  /** Create child if missing.
   * 
   *  @return ID, None if no such node or child could not be created. */
  def getOrCreate(id: Long, child: String) : Option[Long] =
    entry(id, child) map(_ id) orElse create(id, child)
  
  /** @return the ID, None if no such node. */
  def entry(path: String) : Option[TreeEntry] =
    // for maximum performance, we might want to cache full path -> ID eventually
    path.split("/").tail
    .foldLeft(entry(ROOTID))((parent, name) => parent flatMap(parent => entry(parent.id, name) ))
  
  /** @return the child, None if no such node or child. */
  def entry(id: Long, childName: String) : Option[TreeEntry] = children(id) find(_.name == childName)

  /** @return the full path, None if no such node or node is not in tree. */
  def path(id: Long) : Option[String] =
    // for maximum performance, we might want to cache ID -> full path eventually
    if (id == ROOTID) Some(ROOTPATH)
    else entry(id) flatMap(entry => path(entry.parent) map(_ + "/" + entry.name)) 
    
}


object TreeMethods {

  /** @return the parent path for a path, ROOTPATH for the root. */
  def parentPath(path: String) = if (path == ROOTPATH) ROOTPATH else path substring (0, path lastIndexOf "/")
  
  /** @return the element name for a path, ROOTNAME for the root. */
  def nameFromPath(path: String) = if (path == ROOTPATH) ROOTNAME else path substring (1 + path lastIndexOf "/", path length)
  
  /** Entry names must not contain the slash. */
  def isWellformedEntryName(name: String) : Boolean = !(name contains "/")

  /** Sub-paths start with a slash. */
  def isWellformedSubPath(path: String) : Boolean = (path matches "/.*")

  /** Either ROOTPATH for root or a valid sub-path. */
  def isWellformedPath(path: String) : Boolean = (path equals ROOTPATH) || isWellformedSubPath (path)

}

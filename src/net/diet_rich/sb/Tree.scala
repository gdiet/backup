package net.diet_rich.sb

import df._
import Tree._
import TreeDB.{ROOTID, ROOTNAME, ROOTPATH}

trait Tree {
  
  protected def db: TreeDB
  
//  private final def makeFromParent(path: String) : Option[Long] =
//    getOrMake(parentFromPath(path)) flatMap { parent => make(parent, nameFromPath(path)) }
//  
//  /** Create any missing elements in the path, all without data information.
//   *  Note: If None is returned, parts of the path may already have been created.
//   * 
//   *  @return ID if path was created, None if path already existed or could not be created. */
//  def make(path: String) : Option[Long] =
//    get(path) match {
//      case None => makeFromParent(path)
//      case _ => None
//    }
//  
//  /** Create any missing elements in the path, all without data information.
//   *  Note: If None is returned, parts of the path may already have been created.
//   * 
//   *  @return ID or None if entry was missing and could not be created. */
//  def getOrMake(path: String) : Option[Long] =
//    get(path) orElse makeFromParent(path)

  /** Create child.
   * 
   *  @return ID, None if no such node, child already existed or child could not be created. */
  def make(id: Long, child: String) : Option[Long] = db.createNewNode(id, child)

  /** Create child if missing.
   * 
   *  @return ID, None if no such node or child could not be created. */
  def getOrMake(id: Long, child: String) : Option[Long] =
    get(id, child) map(_ id) orElse make(id, child)
  
  /** @return the ID, None if no such node. */
  def get(path: String) : Option[Long] =
    // for maximum performance, we could cache full path -> ID
    path.split("/").tail
    .foldLeft(Option(ROOTID))((parent, name) => parent flatMap( get(_, name) map(_ id) ))
  
  /** @return the child, None if no such node or child. */
  def get(id: Long, childName: String) : Option[IdAndName] = db children id find(_.name == childName)

  /** @return the name, None if no such node. */
  def name(id: Long) : Option[String] = db name id

  /** @return the full path, None if no such node or node is not in tree. */
  def path(id: Long) : Option[String] =
    // for maximum performance, we could cache ID -> full path
    if (id == ROOTID) Some(ROOTPATH)
    else name(id) flatMap(name => db parent id flatMap( path(_) map(_ + "/" + name) ))

  /** @return the children, empty if no such node. */
  def children(id: Long) : Iterable[df.IdAndName] = db children id

}

object Tree {

  /** @return the parent path for a path, ROOTPATH for the root. */
  def parentFromPath(path: String) = if (path == ROOTPATH) ROOTPATH else path substring (0, path lastIndexOf "/")
  
  /** @return the element name for a path, ROOTNAME for the root. */
  def nameFromPath(path: String) = if (path == ROOTPATH) ROOTNAME else path substring (1 + path lastIndexOf "/", path length)
  
  /** Entry names must not contain the slash. */
  def isWellformedEntryName(name: String) : Boolean = !(name contains "/")

  /** Sub-paths start with a slash. */
  def isWellformedSubPath(path: String) : Boolean = (path matches "/.*")

  /** Either ROOTPATH for root or a valid sub-path. */
  def isWellformedPath(path: String) : Boolean = (path equals ROOTPATH) || isWellformedSubPath (path)

}

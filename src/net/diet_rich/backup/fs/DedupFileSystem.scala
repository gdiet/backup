// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.fs

class DedupFileSystem(db: DedupDb) {
  def path(path: String) : DPath = DPath(this, path)
  def file(path: String) : Option[DFile] = fileId(path) map (DFile(this, _))
  def path(id: Long) : Option[DPath] = pathString(id) map (DPath(this, _))
  def file(id: Long) : DFile = DFile(this, id)

  def fileId(path: String) : Option[Long] = {
    require((path equals "") || (path matches "/.*[^/]")) // root or starting but not ending with a slash
    if (path == "") Some(0)
    else
      // FIXME move down to DedupSqlDb since path->id might be interesting to cache
      path.split("/").tail
      .foldLeft(Option(0L))((parent, name) => parent flatMap ( child(_, name) ))
  }
  
  def pathString(id: Long) : Option[String] =
    if (id == 0) Some("")
    else
      name(id) flatMap (name => parent(id) flatMap (parent => pathString(parent) map (parentPath => 
        parentPath + "/" + name
      )))

  def children(id: Long) : List[Long] = db getChildren id
  def exists(id: Long) : Boolean = name(id) isDefined
  def name(id: Long) : Option[String] = db getName id
  def parent(id: Long) : Option[Long] = db getParent id
  def child(id: Long, childName: String) : Option[Long] = {
    require(!childName.contains("/"))
    db getChild (id, childName)
  }
  def mkChild(id: Long, childName: String) : Option[Long] = {
    require(!childName.contains("/"))
    db mkChild (id, childName)
  }
  def rename(id: Long, newName: String) : Boolean ={
    require(!newName.contains("/"))
    db rename (id, newName)
  }
  def delete(id: Long) : Boolean = if (id == 0) false else db delete id
  
  override def toString: String = "fs" // EVENTUALLY write a sensible name
}

/** A file system path identified by the path string. There may not be a file system entry
 *  for the path, and any entry may change all its properties (id, name, parent, children, 
 *  ...) during the path object's lifetime.
 */
case class DPath (val fs: DedupFileSystem, val path: String) {
  require((path equals "") || (path matches "/.*[^/]")) // root or starting but not ending with a slash
  
  def file : Option[DFile] = fs file path
  /** For root, returns root. */
  def parent : DPath = if (isRoot) this else copy( path = path substring (0, path lastIndexOf "/") )
  def isRoot : Boolean = path == ""
  def child(childName: String) : DPath = {
    require(childName matches "/.*[^/]") // starting but not ending with a slash
    DPath(fs, path + "/" + childName)
  }
}

/** A file system entry identified by its ID. The entry may not exist, and it may change
 *  all its properties (name, parent, children, ...) except for its ID during its lifetime.
 */
case class DFile (val fs: DedupFileSystem, val id: Long) {
  def path : Option[DPath] = fs path id
  def children : List[DFile] = fs children id map (copy (fs, _))
  def exists : Boolean = fs exists id
  def name : Option[String] = fs name id
  /** Creating the child will fail if a child with the same name already exists. */
  def mkChild(childName: String) : Option[DFile] = fs mkChild (id, childName) map (copy (fs, _))
  def rename(newName: String) : Boolean = fs rename (id, newName)
  def delete : Boolean = fs delete id
}



// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.fs
import java.util.ConcurrentModificationException

class DedupFileSystem(db: DedupDb) {
  
  // convenience methods for path and file creation
  
  def path(path: String) : DPath = DPath(this, path)
  def file(path: String) : Option[DFile] = fileId(path) map (file _)
  def path(id: Long) : Option[DPath] = pathString(id) map (path _)
  def file(id: Long) : DFile = DFile(this, id)

  // checks used in requirements
  
  private[fs] def isWellformedEntryName(name: String) : Boolean =
    !name.isEmpty && !name.contains("/")
  private[fs] def isWellformedSubPath(path: String) : Boolean =
    ((path matches "/.*[^/]") && !(path contains"//")) // starting but not ending with a slash, no consecutive slashes
  private[fs] def isWellformedPath(path: String) : Boolean =
    (path equals "") || isWellformedSubPath (path) // root or well formed sub path

  // path string handling methods
    
  def parentPath(path: String) = {
    require(isWellformedPath(path))
    if (path == "") "" else path substring (0, path lastIndexOf "/")    
  }
  
  def entryName(path: String) = {
    require(isWellformedPath(path))
    if (path == "") "" else path substring (1 + path lastIndexOf "/", path length)
  }

  def child(path: String, childPath: String) : String = {
    require(isWellformedPath(path))
    require(isWellformedSubPath(childPath))
    path + "/" + childPath
  }

  // methods operating on file IDs
  
  def fileId(path: String) : Option[Long] = {
    require(isWellformedPath(path))
    if (path == "") Some(0) else db getFileId path
  }
  
  def pathString(id: Long) : Option[String] =
    if (id == 0) Some("") else db getPathString id
      
  def children(id: Long) : List[Long] = db getChildren id
  def exists(id: Long) : Boolean = name(id) isDefined
  def name(id: Long) : Option[String] = db getName id
  def parent(id: Long) : Option[Long] = db getParent id
  def child(id: Long, childName: String) : Option[Long] = {
    require(isWellformedEntryName(childName))
    db getChild (id, childName)
  }
  def mkChild(id: Long, childName: String) : Option[Long] = {
    require(isWellformedEntryName(childName))
    db mkChild (id, childName)
  }
  /** Create any required path element. None only if missing e.g. write permission (future extension). */
  def getOrMake(path: String) : Option[Long] = {
    require(isWellformedPath(path))
    fileId(path) orElse {
      getOrMake(parentPath(path)) flatMap (parent =>
        mkChild(parent, entryName(path)) orElse child(parent, entryName(path)) orElse getOrMake(path)
      )
    }
  }
  def rename(id: Long, newName: String) : Boolean ={
    require(id > 0)
    require(isWellformedEntryName(newName))
    db rename (id, newName)
  }
  def delete(id: Long) : Boolean = if (id == 0) false else db delete id
  
  // file data methods
  def setFileData(id: Long, time: Long, data: Long) : Unit = db setFileData (id, time, data)
  def getLastModified(id: Long) : Option[Long] = db getLastModified id
  def getFileDataId(id: Long) : Option[Long] = db getFileDataId id
  def clearFileData(id: Long) : Unit = db clearFileData id
  
  override def toString: String = "fs" // EVENTUALLY write a sensible name
}

/** A file system path identified by the path string. There may not be a file system entry
 *  for the path, and any entry may change all its properties (id, name, parent, children, 
 *  ...) during the path object's lifetime.
 */
case class DPath (val fs: DedupFileSystem, val path: String) {
  require(fs isWellformedPath path, path)
  
  def file : Option[DFile] = fs file path
  /** For root, returns root. */
  def parent : DPath = copy( path = fs parentPath path )
  def name : String = fs entryName path
  def isRoot : Boolean = path == ""
  def child(subPath: String) : DPath = copy(path = fs child (path,subPath))
  def getOrMakeEntry : DFile = DFile(fs, fs getOrMake(path) get)
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
  def setFileData(time: Long, data: Long) : Unit = fs setFileData (id, time, data)
  def getLastModified : Option[Long] = fs getLastModified id
  def getFileDataId : Option[Long] = fs getFileDataId id
  def clearFileData : Unit = fs clearFileData id
}



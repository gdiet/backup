// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.sb

import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import com.google.common.cache.CacheBuilder
import df.IdAndName
import java.lang.{Long => JLong}
import net.diet_rich.util.Configuration._
import scala.collection.immutable.Iterable

/* http://stackoverflow.com/questions/10007994/why-do-i-need-jsr305-to-use-guava-in-scala

Scala requires jsr305: The Scala compiler requires all the types exposed by a class
to be available at compile time, whereas the Java compiler effectively doesn't care.
As to the "official" JSR 305 implementation to use with Guava, I'd use the one they 
declare in their pom.xml (http://code.google.com/p/guava-libraries/source/browse/guava/pom.xml):
<dependency>
    <groupId>com.google.code.findbugs</groupId>
    <artifactId>jsr305</artifactId>
    <version>1.3.9</version>
</dependency>
You can download the jar directly from the Maven Central repository
(http://repo2.maven.org/maven2/com/google/code/findbugs/jsr305/1.3.9). */

class TreeDBCache protected(db: TreeDB with TreeDBInternals, config: StringMap) extends TreeDB {
  protected val cacheSize = config.long("TreeDB.cacheSize")
  
  protected val nodeCache : LoadingCache[JLong, Option[TreeEntry]] =
    CacheBuilder.newBuilder().maximumSize(cacheSize)
    .build(new CacheLoader[JLong, Option[TreeEntry]] {
      override def load(key: JLong) : Option[TreeEntry] = db entry key
    })

  protected val childrenCache : LoadingCache[JLong, Iterable[Long]] =
    CacheBuilder.newBuilder()
    .maximumSize(cacheSize)
    .build(new CacheLoader[JLong, Iterable[Long]] {
      override def load(key: JLong) : Iterable[Long] = db children key map (_.id)
    })

  db.readEvent   subscribe( entry => nodeCache     put (entry id, Some(entry)) )
  db.createEvent subscribe( entry => nodeCache     put (entry id, Some(entry)) )
  db.createEvent subscribe( entry => childrenCache invalidate entry.parent )
  db.changeEvent subscribe{    id => nodeCache     invalidate id }
  db.moveEvent   subscribe(  info => nodeCache     invalidate info.id )
  db.moveEvent   subscribe(  info => childrenCache invalidate info.oldParent )
  db.moveEvent   subscribe(  info => childrenCache invalidate info.newParent )
  db.deleteEvent subscribe( entry => nodeCache     invalidate entry.id )
  db.deleteEvent subscribe( entry => childrenCache invalidate entry.parent )
  
  override def entry(id: Long) : Option[TreeEntry] = nodeCache get id
  override def children(id: Long) : Iterable[TreeEntry] = childrenCache get id flatMap (entry(_))
  override def create(parent: Long, name: String) : Option[Long] = db create (parent, name)
  override def rename(id: Long, newName: String) : Boolean = db rename (id, newName)
  override def setTime(id: Long, newTime: Long) : Boolean = db setTime (id, newTime)
  // EVENTUALLY check performance compared to the "pure" implementations
//  override def move(id: Long, newParent: Long) : Boolean = db move (id, newParent)
//  override def deleteWithChildren(id: Long) : Boolean = db deleteWithChildren(id)
//  override def setData(id: Long, newTime: Option[Long], newData: Option[Long]) : Boolean = db setData (id, newTime, newData)
  override def move(id: Long, newParent: Long) : Boolean = db move (id, entry, newParent)
  override def deleteWithChildren(id: Long) : Boolean = db deleteWithChildren(id, entry, node => children(node) map (_ id))
  override def setData(id: Long, newTime: Option[Long], newData: Option[Long]) : Boolean = db setData (id, entry, newTime, newData)
}

object TreeDBCache {
  def apply(connection: java.sql.Connection, config: StringMap) : TreeDB =
    new TreeDBCache(TreeSqlDB(connection), config)
}

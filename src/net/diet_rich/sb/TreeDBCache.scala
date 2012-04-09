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

class TreeDBCache protected(db: TreeDB with TreeCacheUpdater, config: StringMap) extends TreeDB {
  protected val cacheSize = config.long("TreeDBCache.cacheSize")
  
  db.registerUpdateAdapter(new TreeCacheUpdateAdapter {
    override def created(id: Long, name: String, parent: Long) = {
      childrenCache invalidate parent
      nameCache put(id, Some(name))
      parentCache put(id, Some(parent))
    }
    override def renamed(id: Long, newName: String) = {
      parent(id) foreach (childrenCache invalidate _)
      nameCache put(id, Some(newName))
    }
    override def moved(id: Long, newParent: Long) = {
      childrenCache invalidate newParent
      parent(id) foreach (childrenCache invalidate _)
    }
    override def deleted(id: Long, oldParent: Long) = {
      throw new UnsupportedOperationException
    }
  })

  protected val nameCache : LoadingCache[JLong, Option[String]] =
    CacheBuilder.newBuilder()
    .maximumSize(cacheSize)
    .build(new CacheLoader[JLong, Option[String]] {
      override def load(key: JLong) : Option[String] = db name key
    })

  protected val parentCache : LoadingCache[JLong, Option[JLong]] =
    CacheBuilder.newBuilder()
    .maximumSize(cacheSize)
    .build(new CacheLoader[JLong, Option[JLong]] {
      override def load(key: JLong) : Option[JLong] = db parent key map (x => x)
    })
    
  protected val childrenCache : LoadingCache[JLong, Iterable[IdAndName]] =
    CacheBuilder.newBuilder()
    .maximumSize(cacheSize)
    .build(new CacheLoader[JLong, Iterable[IdAndName]] {
      override def load(key: JLong) : Iterable[IdAndName] = db children key
    })
  
  override def name(id: Long) : Option[String] = nameCache get id
  override def children(id: Long) : Iterable[IdAndName] = childrenCache get id
  override def parent(id: Long) : Option[Long] = parentCache get id map (x => x)
  override def createNewNode(parent: Long, name: String) : Option[Long] = db createNewNode(parent, name)
  override def rename(id: Long, newName: String) : Boolean = db rename(id, newName)
  override def move(id: Long, newParent: Long) : Boolean = db move(id, newParent)
  override def deleteWithChildren(id: Long) : Boolean = db deleteWithChildren id
}

object TreeDBCache {
  def apply(connection: java.sql.Connection, config: StringMap) : TreeDB =
    new TreeDBCache(TreeDB(connection), config)
}

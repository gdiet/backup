// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.sb

import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import com.google.common.cache.CacheBuilder
import java.lang.{Long => JLong}
import net.diet_rich.util.Configuration._
import scala.collection.immutable.Iterable

class DataInfoDBCache protected(infoDB: DataInfoSqlDB, config: StringMap) extends DataInfoDB {
  protected val cacheSize = config.long("DataInfoDB.cacheSize")
  
  protected val cache : LoadingCache[JLong, Option[DataInfo]] =
    CacheBuilder.newBuilder()
    .maximumSize(cacheSize)
    .build(new CacheLoader[JLong, Option[DataInfo]] {
      override def load(key: JLong) : Option[DataInfo] = infoDB readOption key
    })
  
  infoDB.entryEvent subscribe { case (id, info) => cache put (id, Some(info)) }

  override def readOption(id: Long) : Option[DataInfo] = cache get id
  override def create(info: DataInfo) : Long = infoDB create info
  override def update(id: Long, info: DataInfo) : Boolean = infoDB update (id, info)
}

object DataInfoDBCache {
  def apply(connection: java.sql.Connection, config: StringMap) : DataInfoDB =
    new DataInfoDBCache(DataInfoSqlDB(connection), config)
}

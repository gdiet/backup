// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.webdav

import io.milton.resource.CollectionResource
import io.milton.resource.Resource
import java.util.{List => JavaList}
import net.diet_rich.dedup.database.TreeEntryID
import net.diet_rich.util.CallLogging
import scala.collection.JavaConverters._

// note: extend MakeCollectionableResource to enable creating directories (and files?)
// also have a look at FolderResource
class DirectoryResource(name: String, childForName: String => Option[Resource], children: () => Seq[Resource]) extends AbstractResource with CollectionResource with CallLogging {
  def getName(): String = debug("getName()") { name }
  def child(childName: String): Resource = debug(s"child(childName: '$childName')") { childForName(childName) getOrElse null }
  def getChildren(): JavaList[_ <: Resource] = debug("getChildren()") { children() asJava }
}

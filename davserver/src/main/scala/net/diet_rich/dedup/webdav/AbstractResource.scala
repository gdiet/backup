// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.webdav

import java.util.Date

import io.milton.http.{Auth, Request}
import io.milton.http.exceptions.BadRequestException
import io.milton.http.http11.auth.DigestResponse
import io.milton.resource.{DigestResource, PropFindableResource, DeletableResource, MoveableResource, CollectionResource}

import net.diet_rich.dedup.core.FileSystem
import net.diet_rich.dedup.core.values.TreeEntry
import net.diet_rich.dedup.util.{CallLogging, Logging}

trait AbstractResource extends DigestResource with PropFindableResource with Logging with CallLogging {
  val treeEntry: TreeEntry
  val fileSystem: FileSystem

  // DigestResource
  override final def authenticate(digestRequest: DigestResponse): Object = debug(s"authenticate(digestRequest: '$digestRequest')") { digestRequest getUser() }
  override final def isDigestAllowed: Boolean = debug("isDigestAllowed()") { true }
  
  // PropFindableResource
  override final def getCreateDate: Date = debug("getCreateDate()") { getModifiedDate }
  
  // Resource
  override final def getName: String = debug("getName()") { treeEntry.name }
  override final def getUniqueId: String = debug("getUniqueId()") { null }
  override final def authenticate(user: String, password: String): Object = debug(s"authenticate(user: '$user', password: '$password')") { user }
  override final def getRealm: String = debug("getRealm()") { "dedup@diet-rich.net" }
  override final def checkRedirect(request: Request): String = debug(s"checkRedirect(request: '$request')") { null }
}

trait AbstractReadResource { _: AbstractResource =>
  override final def authorise(request: Request, method: Request.Method, auth: Auth): Boolean = debug(s"authorise(request: '$request', method: '$method', auth: '$auth')") { !method.isWrite }
}

trait AbstractWriteResource extends DeletableResource with MoveableResource { _: AbstractResource =>
  override final def authorise(request: Request, method: Request.Method, auth: Auth): Boolean = debug(s"authorise(request: '$request', method: '$method', auth: '$auth')") { true }
  override final def delete(): Unit = { info(s"deleting $treeEntry"); if (!fileSystem.markDeleted(treeEntry id)) log warn s"could not mark deleted $treeEntry" }

  override final def moveTo(destination: CollectionResource, newName: String): Unit = debug(s"moveTo(destination: $destination, newName: $newName)") {
    if (newName.isEmpty) throw new BadRequestException("Rename not possible: New name is empty.")
    if (newName matches ".*[\\?\\*\\/\\\\].*") throw new BadRequestException(s"Rename not possible: New name '$newName' contains illegal characters, one of [?*/\\].")
    destination match {
      case DirectoryResource(`fileSystem`, directory) =>
        if (fileSystem.change(treeEntry id, directory id, newName, treeEntry changed, treeEntry data).isEmpty) log warn s"Could not move $treeEntry to $directory."
      case other => throw new BadRequestException(s"Can't move $this to $other.")
    }
  }
}
